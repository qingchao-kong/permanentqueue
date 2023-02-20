package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.LongRef;
import cn.pockethub.permanentqueue.kafka.common.RecordValidationException;
import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.RequestLocal;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static cn.pockethub.permanentqueue.kafka.message.CompressionCodec.NoCompressionCodec;
import static cn.pockethub.permanentqueue.kafka.message.CompressionCodec.ZStdCompressionCodec;
import static cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion.IBP_2_1_IV0;

public class LogValidator {
    private static final Logger LOG = LoggerFactory.getLogger(LogValidator.class);

    /**
     * Update the offsets for this message set and do further validation on messages including:
     * 1. Messages for compacted topics must have keys
     * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
     * starting from 0.
     * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
     * 4. Declared count of records in DefaultRecordBatch must match number of valid records contained therein.
     * <p>
     * This method will convert messages as necessary to the topic's configured message format version. If no format
     * conversion or value overwriting is required for messages, this method will perform in-place operations to
     * avoid expensive re-compression.
     * <p>
     * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
     * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
     */
    protected static ValidationAndOffsetAssignResult validateMessagesAndAssignOffsets(MemoryRecords records,
                                                                                      TopicPartition topicPartition,
                                                                                      LongRef offsetCounter,
                                                                                      Time time,
                                                                                      Long now,
                                                                                      CompressionCodec sourceCodec,
                                                                                      CompressionCodec targetCodec,
                                                                                      Boolean compactedTopic,
                                                                                      Byte magic,
                                                                                      TimestampType timestampType,
                                                                                      Long timestampDiffMaxMs,
                                                                                      Integer partitionLeaderEpoch,
                                                                                      AppendOrigin origin,
                                                                                      MetadataVersion interBrokerProtocolVersion,
                                                                                      BrokerTopicStats brokerTopicStats,
                                                                                      RequestLocal requestLocal) throws IOException {
        if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
            // check the magic value
            if (!records.hasMatchingMagic(magic)) {
                return convertAndAssignOffsetsNonCompressed(records, topicPartition, offsetCounter, compactedTopic, time, now, timestampType,
                        timestampDiffMaxMs, magic, partitionLeaderEpoch, origin, brokerTopicStats);
            } else {
                // Do in-place validation, offset assignment and maybe set timestamp
                return assignOffsetsNonCompressed(records, topicPartition, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
                        partitionLeaderEpoch, origin, magic, brokerTopicStats);
            }
        } else {
            return validateMessagesAndAssignOffsetsCompressed(records, topicPartition, offsetCounter, time, now, sourceCodec,
                    targetCodec, compactedTopic, magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, origin,
                    interBrokerProtocolVersion, brokerTopicStats, requestLocal);
        }
    }

    private static RecordBatch getFirstBatchAndMaybeValidateNoMoreBatches(MemoryRecords records, CompressionCodec sourceCodec) {
        Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();

        if (!batchIterator.hasNext()) {
            throw new InvalidRecordException("Record batch has no batches at all");
        }

        MutableRecordBatch batch = batchIterator.next();

        // if the format is v2 and beyond, or if the messages are compressed, we should check there's only one batch.
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || sourceCodec != NoCompressionCodec) {
            if (batchIterator.hasNext()) {
                throw new InvalidRecordException("Compressed outer record has more than one batch");
            }
        }

        return batch;
    }

    private static void validateBatch(TopicPartition topicPartition,
                                      RecordBatch firstBatch,
                                      RecordBatch batch,
                                      AppendOrigin origin,
                                      Byte toMagic,
                                      BrokerTopicStats brokerTopicStats) {
        // batch magic byte should have the same magic as the first batch
        if (firstBatch.magic() != batch.magic()) {
            brokerTopicStats.allTopicsStats.invalidMagicNumberRecordsPerSec().mark();
            String msg = String.format("Batch magic %s is not the same as the first batch'es magic byte %s in topic partition %s.",
                    batch.magic(), firstBatch.magic(), topicPartition);
            throw new InvalidRecordException(msg);
        }

        if (origin == AppendOrigin.Client) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                long countFromOffsets = batch.lastOffset() - batch.baseOffset() + 1;
                if (countFromOffsets <= 0) {
                    brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
                    String msg = String.format("Batch has an invalid offset range: [%s, %s] in topic partition %s.",
                            batch.baseOffset(), batch.lastOffset(), topicPartition);
                    throw new InvalidRecordException(msg);
                }

                // v2 and above messages always have a non-null count
                Integer count = batch.countOrNull();
                if (count <= 0) {
                    brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
                    String msg = String.format("Invalid reported count for record batch: %s in topic partition %s.",
                            count, topicPartition);
                    throw new InvalidRecordException(msg);
                }

                if (countFromOffsets != batch.countOrNull()) {
                    brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
                    String msg = String.format("Inconsistent batch offset range [%s, %s] and count of records %s in topic partition %s.",
                            batch.baseOffset(), batch.lastOffset(), count, topicPartition);
                    throw new InvalidRecordException(msg);
                }
            }

            if (batch.isControlBatch()) {
                brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
                String msg = String.format("Clients are not allowed to write control records in topic partition %s.", topicPartition);
                throw new InvalidRecordException(msg);
            }

            if (batch.hasProducerId() && batch.baseSequence() < 0) {
                brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
                String msg = String.format("Invalid sequence number %s in record batch with producerId %s in topic partition %s.",
                        batch.baseSequence(), batch.producerId(), topicPartition);
                throw new InvalidRecordException(msg);
            }
        }

        if (batch.isTransactional() && toMagic < RecordBatch.MAGIC_VALUE_V2) {
            String msg = String.format("Transactional records cannot be used with magic version %s", toMagic);
            throw new UnsupportedForMessageFormatException(msg);
        }

        if (batch.hasProducerId() && toMagic < RecordBatch.MAGIC_VALUE_V2) {
            String msg = String.format("Idempotent records cannot be used with magic version %s", toMagic);
            throw new UnsupportedForMessageFormatException(msg);
        }
    }

    private static Optional<ApiRecordError> validateRecord(RecordBatch batch,
                                                           TopicPartition topicPartition,
                                                           Record record,
                                                           Integer batchIndex,
                                                           Long now,
                                                           TimestampType timestampType,
                                                           Long timestampDiffMaxMs,
                                                           Boolean compactedTopic,
                                                           BrokerTopicStats brokerTopicStats) {
        if (!record.hasMagic(batch.magic())) {
            brokerTopicStats.allTopicsStats.invalidMagicNumberRecordsPerSec().mark();
            String msg = String.format("Record %s's magic does not match outer magic %s in topic partition %s.",
                    record, batch.magic(), topicPartition);
            return Optional.of(new ApiRecordError(Errors.INVALID_RECORD, new ProduceResponse.RecordError(batchIndex, msg)));
        }

        // verify the record-level CRC only if this is one of the deep entries of a compressed message
        // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
        // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
        // there is no record-level CRC to check.
        if (batch.magic() <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed()) {
            try {
                record.ensureValid();
            } catch (InvalidRecordException e) {
                brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec().mark();
                String msg = String.format("%s in topic partition %s.", e.getMessage(), topicPartition);
                throw new CorruptRecordException(msg);
            }
        }

        Optional<ApiRecordError> result = validateKey(record, batchIndex, topicPartition, compactedTopic, brokerTopicStats);
        if (result.isPresent()) {
            return result;
        } else {
            return validateTimestamp(batch, record, batchIndex, now, timestampType, timestampDiffMaxMs);
        }
    }

    private static ValidationAndOffsetAssignResult convertAndAssignOffsetsNonCompressed(MemoryRecords records,
                                                                                        TopicPartition topicPartition,
                                                                                        LongRef offsetCounter,
                                                                                        Boolean compactedTopic,
                                                                                        Time time,
                                                                                        Long now,
                                                                                        TimestampType timestampType,
                                                                                        Long timestampDiffMaxMs,
                                                                                        Byte toMagicValue,
                                                                                        Integer partitionLeaderEpoch,
                                                                                        AppendOrigin origin,
                                                                                        BrokerTopicStats brokerTopicStats) {
        long startNanos = time.nanoseconds();
        int sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.getValue(),
                CompressionType.NONE, records.records());

        MutableRecordBatch first = records.batches().iterator().next();
        long producerId = first.producerId();
        short producerEpoch = first.producerEpoch();
        int sequence = first.baseSequence();
        boolean isTransactional = first.isTransactional();

        // The current implementation of BufferSupplier is naive and works best when the buffer size
        // cardinality is low, so don't use it here
        ByteBuffer newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion);
        MemoryRecordsBuilder builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
                offsetCounter.getValue(), now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch);

        RecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec);

        records.batches().forEach(batch -> {
            validateBatch(topicPartition, firstBatch, batch, origin, toMagicValue, brokerTopicStats);

            ArrayList<ApiRecordError> recordErrors = new ArrayList<ApiRecordError>(0);

            int batchIndex = 0;
            for (Record record : batch) {
                validateRecord(batch, topicPartition, record, batchIndex, now, timestampType,
                        timestampDiffMaxMs, compactedTopic, brokerTopicStats)
                        .ifPresent(recordErrors::add);
                // we fail the batch if any record fails, so we stop appending if any record fails
                if (recordErrors.isEmpty()) {
                    builder.appendWithOffset(offsetCounter.getAndIncrement(), record);
                }
                batchIndex++;
            }

            processRecordErrors(recordErrors);
        });

        MemoryRecords convertedRecords = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        RecordConversionStats recordConversionStats = new RecordConversionStats(builder.uncompressedBytesWritten(),
                builder.numRecords(), time.nanoseconds() - startNanos);
        return new ValidationAndOffsetAssignResult(
                convertedRecords,
                info.maxTimestamp,
                info.shallowOffsetOfMaxTimestamp,
                true,
                recordConversionStats);
    }

    public static ValidationAndOffsetAssignResult assignOffsetsNonCompressed(MemoryRecords records,
                                                                             TopicPartition topicPartition,
                                                                             LongRef offsetCounter,
                                                                             Long now,
                                                                             Boolean compactedTopic,
                                                                             TimestampType timestampType,
                                                                             Long timestampDiffMaxMs,
                                                                             Integer partitionLeaderEpoch,
                                                                             AppendOrigin origin,
                                                                             Byte magic,
                                                                             BrokerTopicStats brokerTopicStats) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        long initialOffset = offsetCounter.getValue();

        RecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec);

        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(topicPartition, firstBatch, batch, origin, magic, brokerTopicStats);

            long maxBatchTimestamp = RecordBatch.NO_TIMESTAMP;
            long offsetOfMaxBatchTimestamp = -1L;

            ArrayList<ApiRecordError> recordErrors = new ArrayList<>(0);
            // This is a hot path and we want to avoid any unnecessary allocations.
            // That said, there is no benefit in using `skipKeyValueIterator` for the uncompressed
            // case since we don't do key/value copies in this path (we just slice the ByteBuffer)
            int batchIndex = 0;
            for (Record record : batch) {
                validateRecord(batch, topicPartition, record, batchIndex, now, timestampType,
                        timestampDiffMaxMs, compactedTopic, brokerTopicStats)
                        .ifPresent(recordErrors::add);

                long offset = offsetCounter.getAndIncrement();
                if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && record.timestamp() > maxBatchTimestamp) {
                    maxBatchTimestamp = record.timestamp();
                    offsetOfMaxBatchTimestamp = offset;
                }
                batchIndex += 1;
            }

            processRecordErrors(recordErrors);

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
                maxTimestamp = maxBatchTimestamp;
                offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp;
            }

            batch.setLastOffset(offsetCounter.getValue() - 1);

            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                batch.setPartitionLeaderEpoch(partitionLeaderEpoch);
            }

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0) {
                if (timestampType == TimestampType.LOG_APPEND_TIME) {
                    batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now);
                } else {
                    batch.setMaxTimestamp(timestampType, maxBatchTimestamp);
                }
            }
        }

        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            maxTimestamp = now;
            if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                offsetOfMaxTimestamp = offsetCounter.getValue() - 1;
            } else {
                offsetOfMaxTimestamp = initialOffset;
            }
        }

        return new ValidationAndOffsetAssignResult(
                records,
                maxTimestamp,
                offsetOfMaxTimestamp,
                false,
                RecordConversionStats.EMPTY);
    }

    private static Optional<ApiRecordError> validateRecordCompression(Integer batchIndex, Record record,CompressionCodec sourceCodec) {
        if (sourceCodec != NoCompressionCodec && record.isCompressed()) {
            String msg = String.format("Compressed outer record should not have an inner record with a compression attribute set: %s", record);
            return Optional.of(new ApiRecordError(Errors.INVALID_RECORD, new ProduceResponse.RecordError(batchIndex, msg)));
        } else {
            return Optional.empty();
        }
    }

    /**
     * We cannot do in place assignment in one of the following situations:
     * 1. Source and target compression codec are different
     * 2. When the target magic is not equal to batches' magic, meaning format conversion is needed.
     * 3. When the target magic is equal to V0, meaning absolute offsets need to be re-assigned.
     */
    public static ValidationAndOffsetAssignResult validateMessagesAndAssignOffsetsCompressed(MemoryRecords records,
                                                                                             TopicPartition topicPartition,
                                                                                             LongRef offsetCounter,
                                                                                             Time time,
                                                                                             Long now,
                                                                                             CompressionCodec sourceCodec,
                                                                                             CompressionCodec targetCodec,
                                                                                             Boolean compactedTopic,
                                                                                             Byte toMagic,
                                                                                             TimestampType timestampType,
                                                                                             Long timestampDiffMaxMs,
                                                                                             Integer partitionLeaderEpoch,
                                                                                             AppendOrigin origin,
                                                                                             MetadataVersion interBrokerProtocolVersion,
                                                                                             BrokerTopicStats brokerTopicStats,
                                                                                             RequestLocal requestLocal) {

        if (targetCodec == ZStdCompressionCodec && interBrokerProtocolVersion.isLessThan(IBP_2_1_IV0)) {
            throw new UnsupportedCompressionTypeException("Produce requests to inter.broker.protocol.version < 2.1 broker " +
                    "are not allowed to use ZStandard compression");
        }

        // No in place assignment situation 1
        boolean inPlaceAssignment = sourceCodec == targetCodec;

        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        LongRef expectedInnerOffset = new LongRef(0L);
        ArrayList<Record> validatedRecords = new ArrayList<>();

        int uncompressedSizeInBytes = 0;

        // Assume there's only one batch with compressed memory records; otherwise, return InvalidRecordException
        // One exception though is that with format smaller than v2, if sourceCodec is noCompression, then each batch is actually
        // a single record so we'd need to special handle it by creating a single wrapper batch that includes all the records
        RecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, sourceCodec);

        // No in place assignment situation 2 and 3: we only need to check for the first batch because:
        //  1. For most cases (compressed records, v2, for example), there's only one batch anyways.
        //  2. For cases that there may be multiple batches, all batches' magic should be the same.
        if (firstBatch.magic() != toMagic || toMagic == RecordBatch.MAGIC_VALUE_V0) {
            inPlaceAssignment = false;
        }

        // Do not compress control records unless they are written compressed
        if (sourceCodec == NoCompressionCodec && firstBatch.isControlBatch()) {
            inPlaceAssignment = true;
        }

        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(topicPartition, firstBatch, batch, origin, toMagic, brokerTopicStats);
            uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType());

            // if we are on version 2 and beyond, and we know we are going for in place assignment,
            // then we can optimize the iterator to skip key / value / headers since they would not be used at all
            CloseableIterator<Record> recordsIterator = (inPlaceAssignment && firstBatch.magic() >= RecordBatch.MAGIC_VALUE_V2) ?
                    batch.skipKeyValueIterator(requestLocal.getBufferSupplier())
                    : batch.streamingIterator(requestLocal.getBufferSupplier());

            try {
                ArrayList<ApiRecordError> recordErrors = new ArrayList<>(0);
                // this is a hot path and we want to avoid any unnecessary allocations.
                int batchIndex = 0;
                while (recordsIterator.hasNext()) {
                    Record record = recordsIterator.next();
                    long expectedOffset = expectedInnerOffset.getAndIncrement();
                    Optional<ApiRecordError> recordError = validateRecordCompression(batchIndex, record,sourceCodec);
                    if (!recordError.isPresent()) {
                        recordError = validateRecord(batch, topicPartition, record, batchIndex, now,
                                timestampType, timestampDiffMaxMs, compactedTopic, brokerTopicStats);
                        if (!recordError.isPresent()) {
                            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
                                if (record.timestamp() > maxTimestamp) {
                                    maxTimestamp = record.timestamp();
                                }

                                // Some older clients do not implement the V1 internal offsets correctly.
                                // Historically the broker handled this by rewriting the batches rather
                                // than rejecting the request. We must continue this handling here to avoid
                                // breaking these clients.
                                if (record.offset() != expectedOffset) {
                                    inPlaceAssignment = false;
                                }
                            }
                            recordError = Optional.empty();
                        }
                    }

                    if (recordError.isPresent()) {
                        ApiRecordError e = recordError.get();
                        recordErrors.add(e);
                    } else {
                        uncompressedSizeInBytes += record.sizeInBytes();
                        validatedRecords.add(record);
                    }
                    batchIndex += 1;
                }
                processRecordErrors(recordErrors);
            } finally {
                recordsIterator.close();
            }
        }

        if (!inPlaceAssignment) {
            // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
            // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
            // with older magic versions, there will never be a producer id, etc.
            MutableRecordBatch first = records.batches().iterator().next();
            long producerId = first.producerId();
            short producerEpoch = first.producerEpoch();
            int sequence = first.baseSequence();
            boolean isTransactional = first.isTransactional();
            return buildRecordsAndAssignOffsets(toMagic, offsetCounter, time, timestampType, CompressionType.forId(targetCodec.getCodec()),
                    now, validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch,
                    uncompressedSizeInBytes);
        } else {
            // we can update the batch only and write the compressed payload as is;
            // again we assume only one record batch within the compressed set
            MutableRecordBatch batch = records.batches().iterator().next();
            long lastOffset = offsetCounter.addAndGet(new Long(validatedRecords.size())) - 1;

            batch.setLastOffset(lastOffset);

            if (timestampType == TimestampType.LOG_APPEND_TIME) {
                maxTimestamp = now;
            }

            if (toMagic >= RecordBatch.MAGIC_VALUE_V1) {
                batch.setMaxTimestamp(timestampType, maxTimestamp);
            }

            if (toMagic >= RecordBatch.MAGIC_VALUE_V2) {
                batch.setPartitionLeaderEpoch(partitionLeaderEpoch);
            }

            RecordConversionStats recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes, 0, 0);
            return new ValidationAndOffsetAssignResult(records,
                    maxTimestamp,
                    lastOffset,
                    false,
                    recordConversionStats);
        }
    }

    private static ValidationAndOffsetAssignResult buildRecordsAndAssignOffsets(Byte magic,
                                                                                LongRef offsetCounter,
                                                                                Time time,
                                                                                TimestampType timestampType,
                                                                                CompressionType compressionType,
                                                                                Long logAppendTime,
                                                                                List<Record> validatedRecords,
                                                                                Long producerId,
                                                                                Short producerEpoch,
                                                                                Integer baseSequence,
                                                                                Boolean isTransactional,
                                                                                Integer partitionLeaderEpoch,
                                                                                Integer uncompressedSizeInBytes) {
        long startNanos = time.nanoseconds();
        int estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.getValue(), compressionType,
                validatedRecords);
        // The current implementation of BufferSupplier is naive and works best when the buffer size
        // cardinality is low, so don't use it here
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.getValue(),
                logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch);

        for (Record record : validatedRecords) {
            builder.appendWithOffset(offsetCounter.getAndIncrement(), record);
        }

        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();

        // This is not strictly correct, it represents the number of records where in-place assignment is not possible
        // instead of the number of records that were converted. It will over-count cases where the source and target are
        // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
        // to rebuild the records (including recompression if enabled).
        int conversionCount = builder.numRecords();
        RecordConversionStats recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes + builder.uncompressedBytesWritten(),
                conversionCount, time.nanoseconds() - startNanos);

        return new ValidationAndOffsetAssignResult(
                records,
                info.maxTimestamp,
                info.shallowOffsetOfMaxTimestamp,
                true,
                recordConversionStats);
    }

    private static Optional<ApiRecordError> validateKey(Record record,
                                                        Integer batchIndex,
                                                        TopicPartition topicPartition,
                                                        Boolean compactedTopic,
                                                        BrokerTopicStats brokerTopicStats) {
        if (compactedTopic && !record.hasKey()) {
            brokerTopicStats.allTopicsStats.noKeyCompactedTopicRecordsPerSec().mark();
            String msg = String.format("Compacted topic cannot accept message without key in topic partition %s.", topicPartition);
            return Optional.of(new ApiRecordError(Errors.INVALID_RECORD, new ProduceResponse.RecordError(batchIndex, msg)));
        } else {
            return Optional.empty();
        }
    }

    private static Optional<ApiRecordError> validateTimestamp(RecordBatch batch,
                                                              Record record,
                                                              Integer batchIndex,
                                                              Long now,
                                                              TimestampType timestampType,
                                                              Long timestampDiffMaxMs) {
        if (timestampType == TimestampType.CREATE_TIME
                && record.timestamp() != RecordBatch.NO_TIMESTAMP
                && Math.abs(record.timestamp() - now) > timestampDiffMaxMs) {
            String msg = String.format("Timestamp %s of message with offset %s is out of range. The timestamp should be within [%s, %s]",
                    record.timestamp(), record.offset(), now - timestampDiffMaxMs, now + timestampDiffMaxMs);
            return Optional.of(new ApiRecordError(Errors.INVALID_TIMESTAMP, new ProduceResponse.RecordError(batchIndex, msg)));
        } else if (batch.timestampType() == TimestampType.LOG_APPEND_TIME) {
            String msg = String.format("Invalid timestamp type in message %s. Producer should not set timestamp " +
                    "type to LogAppendTime.", record);
            return Optional.of(new ApiRecordError(Errors.INVALID_TIMESTAMP, new ProduceResponse.RecordError(batchIndex, msg)));
        } else {
            return Optional.empty();
        }
    }

    private static void processRecordErrors(List<ApiRecordError> recordErrors) {
        if (CollectionUtils.isNotEmpty(recordErrors)) {
            List<ProduceResponse.RecordError> errors = recordErrors.stream().map(ApiRecordError::getRecordError).collect(Collectors.toList());
            if (recordErrors.stream().anyMatch(error->error.apiError== Errors.INVALID_TIMESTAMP)) {
                throw new RecordValidationException(new InvalidTimestampException("One or more records have been rejected due to invalid timestamp"),
                        errors);
            } else {
                String msg = String.format("One or more records have been rejected due to %s record errors " +
                        "in total, and only showing the first three errors at most: %s", errors.size(), errors.subList(0, Math.min(errors.size(), 3)));
                throw new RecordValidationException(new InvalidRecordException(msg), errors);
            }
        }
    }

    @Getter
    public static class ApiRecordError {

        private Errors apiError;
        private ProduceResponse.RecordError recordError;

        public ApiRecordError(Errors apiError, ProduceResponse.RecordError recordError) {
            this.apiError = apiError;
            this.recordError = recordError;
        }
    }
}
