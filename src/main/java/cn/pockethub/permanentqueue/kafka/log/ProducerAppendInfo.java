package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * This class is used to validate the records appended by a given producer before they are written to the log.
 * It is initialized with the producer's state after the last successful append, and transitively validates the
 * sequence numbers and epochs of each new record. Additionally, this class accumulates transaction metadata
 * as the incoming records are validated.
 */
@Getter
public class ProducerAppendInfo {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerAppendInfo.class);

    private TopicPartition topicPartition;
    private Long producerId;
    private ProducerStateEntry currentEntry;
    private AppendOrigin origin;

    private final List<TxnMetadata> transactions = new ArrayList<>();
    private final ProducerStateEntry updatedEntry = ProducerStateEntry.empty(producerId);

    /**
     * @param topicPartition
     * @param producerId     The id of the producer appending to the log
     * @param currentEntry   The current entry associated with the producer id which contains metadata for a fixed number of
     *                       the most recent appends made by the producer. Validation of the first incoming append will
     *                       be made against the latest append in the current entry. New appends will replace older appends
     *                       in the current entry so that the space overhead is constant.
     * @param origin         Indicates the origin of the append which implies the extent of validation. For example, offset
     *                       commits, which originate from the group coordinator, do not have sequence numbers and therefore
     *                       only producer epoch validation is done. Appends which come through replication are not validated
     *                       (we assume the validation has already been done) and appends from clients require full validation.
     */
    public ProducerAppendInfo(TopicPartition topicPartition,
                              Long producerId,
                              ProducerStateEntry currentEntry,
                              AppendOrigin origin) {
        this.topicPartition = topicPartition;
        this.producerId = producerId;
        this.currentEntry = currentEntry;
        this.origin = origin;

        updatedEntry.setProducerEpoch(currentEntry.getProducerEpoch());
        updatedEntry.setCoordinatorEpoch(currentEntry.getCoordinatorEpoch());
        updatedEntry.setLastTimestamp(currentEntry.getLastTimestamp());
        updatedEntry.setCurrentTxnFirstOffset(currentEntry.getCurrentTxnFirstOffset());
    }

    private void maybeValidateDataBatch(Short producerEpoch, Integer firstSeq, Long offset) {
        checkProducerEpoch(producerEpoch, offset);
        if (origin == AppendOrigin.Client) {
            checkSequence(producerEpoch, firstSeq, offset);
        }
    }

    private void checkProducerEpoch(Short producerEpoch, Long offset) {
        if (producerEpoch < updatedEntry.getProducerEpoch()) {
            String message = String.format("Epoch of producer %s at offset %s in %s is %s, which is smaller than the last seen epoch %s",
                    producerId, offset, topicPartition, producerEpoch, updatedEntry.getProducerEpoch());

            if (origin == AppendOrigin.Replication) {
                LOG.warn(message);
            } else {
                // Starting from 2.7, we replaced ProducerFenced error with InvalidProducerEpoch in the
                // producer send response callback to differentiate from the former fatal exception,
                // letting client abort the ongoing transaction and retry.
                throw new InvalidProducerEpochException(message);
            }
        }
    }

    private void checkSequence(Short producerEpoch, Integer appendFirstSeq, Long offset) {
        if (producerEpoch != updatedEntry.getProducerEpoch()) {
            if (appendFirstSeq != 0) {
                if (updatedEntry.getProducerEpoch() != RecordBatch.NO_PRODUCER_EPOCH) {
                    String msg = String.format("Invalid sequence number for new epoch of producer %s at offset %s in partition %s: %s (request epoch), %s (seq. number), %s (current producer epoch)",
                            producerId, offset, topicPartition, producerEpoch, appendFirstSeq, updatedEntry.getProducerEpoch());
                    throw new OutOfOrderSequenceException(msg);
                }
            }
        } else {
            int currentLastSeq = 0;
            if (!updatedEntry.isEmpty()) {
                currentLastSeq = updatedEntry.lastSeq();
            } else if (producerEpoch == currentEntry.getProducerEpoch()) {
                currentEntry.lastSeq();
            } else {
                currentLastSeq = RecordBatch.NO_SEQUENCE;
            }

            // If there is no current producer epoch (possibly because all producer records have been deleted due to
            // retention or the DeleteRecords API) accept writes with any sequence number
            if (!(currentEntry.getProducerEpoch() == RecordBatch.NO_PRODUCER_EPOCH || inSequence(currentLastSeq, appendFirstSeq))) {
                String msg = String.format("Out of order sequence number for producer %s at offset %s in partition %s: %s (incoming seq. number), %s (current end sequence number)",
                        producerId, offset, topicPartition, appendFirstSeq, currentLastSeq);
                throw new OutOfOrderSequenceException(msg);
            }
        }
    }

    private Boolean inSequence(Integer lastSeq, Integer nextSeq) {
        return nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Integer.MAX_VALUE);
    }

    public Optional<CompletedTxn> append(RecordBatch batch, Optional<LogOffsetMetadata> firstOffsetMetadataOpt) {
        if (batch.isControlBatch()) {
            Iterator<Record> recordIterator = batch.iterator();
            if (recordIterator.hasNext()) {
                Record record = recordIterator.next();
                EndTransactionMarker endTxnMarker = EndTransactionMarker.deserialize(record);
                return appendEndTxnMarker(endTxnMarker, batch.producerEpoch(), batch.baseOffset(), record.timestamp());
            } else {
                // An empty control batch means the entire transaction has been cleaned from the log, so no need to append
                return Optional.empty();
            }
        } else {
            LogOffsetMetadata firstOffsetMetadata = firstOffsetMetadataOpt.orElse(new LogOffsetMetadata(batch.baseOffset()));
            appendDataBatch(batch.producerEpoch(), batch.baseSequence(), batch.lastSequence(), batch.maxTimestamp(),
                    firstOffsetMetadata, batch.lastOffset(), batch.isTransactional());
            return Optional.empty();
        }
    }

    public void appendDataBatch(Short epoch,
                                Integer firstSeq,
                                Integer lastSeq,
                                Long lastTimestamp,
                                LogOffsetMetadata firstOffsetMetadata,
                                Long lastOffset,
                                Boolean isTransactional) {
        long firstOffset = firstOffsetMetadata.getMessageOffset();
        maybeValidateDataBatch(epoch, firstSeq, firstOffset);
        updatedEntry.addBatch(epoch, lastSeq, lastOffset, new Long(lastOffset - firstOffset).intValue(), lastTimestamp);

        Optional<Long> currentTxnFirstOffset = updatedEntry.getCurrentTxnFirstOffset();
        if (currentTxnFirstOffset.isPresent() && !isTransactional) {
            // Received a non-transactional message while a transaction is active
            String msg = String.format("Expected transactional write from producer %s at offset %s in partition %s",
                    producerId, firstOffsetMetadata, topicPartition);
            throw new InvalidTxnStateException(msg);
        } else if (!currentTxnFirstOffset.isPresent() && isTransactional) {
            // Began a new transaction
            updatedEntry.setCurrentTxnFirstOffset(Optional.of(firstOffset));
            transactions.add(new TxnMetadata(producerId, firstOffsetMetadata));
        } else {
            // nothing to do
        }
    }

    private void checkCoordinatorEpoch(EndTransactionMarker endTxnMarker, Long offset) {
        if (updatedEntry.getCoordinatorEpoch() > endTxnMarker.coordinatorEpoch()) {
            if (origin == AppendOrigin.Replication) {
                LOG.info("Detected invalid coordinator epoch for producerId {} at offset {} in partition {}: {} " +
                                "is older than previously known coordinator epoch {}",
                        producerId, offset, topicPartition, endTxnMarker.coordinatorEpoch(), updatedEntry.getCoordinatorEpoch());
            } else {
                String msg = String.format("Invalid coordinator epoch for producerId %s at " +
                                "offset %s in partition %s: %s " +
                                "(zombie), %s (current)",
                        producerId, offset, topicPartition, endTxnMarker.coordinatorEpoch(), updatedEntry.getCoordinatorEpoch());
                throw new TransactionCoordinatorFencedException(msg);
            }
        }
    }

    public Optional<CompletedTxn> appendEndTxnMarker(EndTransactionMarker endTxnMarker,
                                                     Short producerEpoch,
                                                     Long offset,
                                                     Long timestamp) {
        checkProducerEpoch(producerEpoch, offset);
        checkCoordinatorEpoch(endTxnMarker, offset);

        // Only emit the `CompletedTxn` for non-empty transactions. A transaction marker
        // without any associated data will not have any impact on the last stable offset
        // and would not need to be reflected in the transaction index.
        Optional<CompletedTxn> completedTxn = updatedEntry.getCurrentTxnFirstOffset().map(firstOffset -> {
            return new CompletedTxn(producerId, firstOffset, offset, endTxnMarker.controlType() == ControlRecordType.ABORT);
        });

        updatedEntry.maybeUpdateProducerEpoch(producerEpoch);
        updatedEntry.setCurrentTxnFirstOffset(Optional.empty());
        updatedEntry.setCoordinatorEpoch(endTxnMarker.coordinatorEpoch());
        updatedEntry.setLastTimestamp(timestamp);

        return completedTxn;
    }

    public ProducerStateEntry toEntry() {
        return updatedEntry;
    }

    public List<TxnMetadata> startedTransactions() {
        return transactions;
    }

    @Override
    public String toString() {
        return String.format("ProducerAppendInfo(" +
                        "producerId=%s, " +
                        "producerEpoch=%s, " +
                        "firstSequence=%s, " +
                        "lastSequence=%s, " +
                        "currentTxnFirstOffset=%s, " +
                        "coordinatorEpoch=%s, " +
                        "lastTimestamp=%s, " +
                        "startedTransactions=%s)",
                producerId, updatedEntry.getProducerEpoch(), updatedEntry.firstSeq(), updatedEntry.lastSeq(), updatedEntry.getCurrentTxnFirstOffset(),
                updatedEntry.getCoordinatorEpoch(), updatedEntry.getLastTimestamp(), transactions);
    }
}
