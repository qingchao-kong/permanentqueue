package cn.pockethub.permanentqueue.kafka.log;

import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class encapsulates a thread-safe navigable map of LogSegment instances and provides the
 * required read and write behavior on the map.
 */
public class LogSegments {

    private final TopicPartition topicPartition;
    /* the segments of the log with key being LogSegment base offset and value being a LogSegment */
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    /**
     * @param topicPartition the TopicPartition associated with the segments
     *                       (useful for logging purposes)
     */
    public LogSegments(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * @return true if the segments are empty, false otherwise.
     */
//    @threadsafe
    public Boolean isEmpty() {
        return MapUtils.isEmpty(segments);
    }

    /**
     * @return true if the segments are non-empty, false otherwise.
     */
//    @threadsafe
    public Boolean nonEmpty() {
        return !isEmpty();
    }

    /**
     * Add the given segment, or replace an existing entry.
     *
     * @param segment the segment to add
     */
//    @threadsafe
    public LogSegment add(LogSegment segment) {
        return this.segments.put(segment.getBaseOffset(), segment);
    }

    /**
     * Remove the segment at the provided offset.
     *
     * @param offset the offset to be removed
     */
//    @threadsafe
    public void remove(Long offset) {
        segments.remove(offset);
    }

    /**
     * Clears all entries.
     */
//    @threadsafe
    public void clear() {
        segments.clear();
    }

    /**
     * Close all segments.
     */
    public void close() {
        for (LogSegment seg : values()) {
            seg.close();
        }
    }

    /**
     * Close the handlers for all segments.
     */
    public void closeHandlers() {
        for (LogSegment seg : values()) {
            seg.closeHandlers();
        }
    }

    /**
     * Update the directory reference for the log and indices of all segments.
     *
     * @param dir the renamed directory
     */
    public void updateParentDir(File dir) {
        for (LogSegment seg : values()) {
            seg.updateParentDir(dir);
        }
    }

    /**
     * Take care! this is an O(n) operation, where n is the number of segments.
     *
     * @return The number of segments.
     */
//    @threadsafe
    public Integer numberOfSegments() {
        return segments.size();
    }

    /**
     * @return the base offsets of all segments
     */
    public Iterable<Long> baseOffsets() {
        return segments.values().stream().map(LogSegment::getBaseOffset).collect(Collectors.toList());
    }

    /**
     * @param offset the segment to be checked
     * @return true if a segment exists at the provided offset, false otherwise.
     */
//    @threadsafe
    public Boolean contains(Long offset) {
        return segments.containsKey(offset);
    }

    /**
     * Retrieves a segment at the specified offset.
     *
     * @param offset the segment to be retrieved
     * @return the segment if it exists, otherwise None.
     */
////    @threadsafe
    public Optional<LogSegment> get(Long offset) {
        if (segments.containsKey(offset)) {
            return Optional.of(segments.get(offset));
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return an iterator to the log segments ordered from oldest to newest.
     */
    public Collection<LogSegment> values() {
        return segments.values();
    }

    /**
     * @return An iterator to all segments beginning with the segment that includes "from" and ending
     * with the segment that includes up to "to-1" or the end of the log (if to > end of log).
     */
    public Collection<LogSegment> values(Long from, Long to) {
        if (Objects.equals(from, to)) {
            // Handle non-segment-aligned empty sets
            return new ArrayList<>(0);
        } else if (to < from) {
            String msg = String.format("Invalid log segment range: requested segments in %s " +
                    "from offset %s which is greater than limit offset %s", topicPartition, from, to);
            throw new IllegalArgumentException(msg);
        } else {
            return segments.subMap(from, true, to, false).values();
        }
    }

    public Iterable<LogSegment> nonActiveLogSegmentsFrom(Long from) {
        LogSegment activeSegment = lastSegment().get();
        if (from > activeSegment.getBaseOffset()) {
            return new HashSet<>();
        } else {
            return values(from, activeSegment.getBaseOffset());
        }
    }

    /**
     * @return the entry associated with the greatest offset less than or equal to the given offset,
     * if it exists.
     */
//    @threadsafe
    private Optional<Map.Entry<Long, LogSegment>> floorEntry(Long offset) {
        return Optional.ofNullable(segments.floorEntry(offset));
    }

    /**
     * @return the log segment with the greatest offset less than or equal to the given offset,
     * if it exists.
     */
//    @threadsafe
    public Optional<LogSegment> floorSegment(Long offset) {
        return floorEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * @return the entry associated with the greatest offset strictly less than the given offset,
     * if it exists.
     */
//    @threadsafe
    private Optional<Map.Entry<Long, LogSegment>> lowerEntry(Long offset) {
        return Optional.ofNullable(segments.lowerEntry(offset));
    }

    /**
     * @return the log segment with the greatest offset strictly less than the given offset,
     * if it exists.
     */
//    @threadsafe
    public Optional<LogSegment> lowerSegment(Long offset) {
        return lowerEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * @return the entry associated with the smallest offset strictly greater than the given offset,
     * if it exists.
     */
//    @threadsafe
    public Optional<Map.Entry<Long, LogSegment>> higherEntry(Long offset) {
        return Optional.ofNullable(segments.higherEntry(offset));
    }

    /**
     * @return the log segment with the smallest offset strictly greater than the given offset,
     * if it exists.
     */
//    @threadsafe
    public Optional<LogSegment> higherSegment(Long offset) {
        return higherEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * @return the entry associated with the smallest offset, if it exists.
     */
//    @threadsafe
    public Optional<Map.Entry<Long, LogSegment>> firstEntry() {
        return Optional.ofNullable(segments.firstEntry());
    }

    /**
     * @return the log segment associated with the smallest offset, if it exists.
     */
//    @threadsafe
    public Optional<LogSegment> firstSegment() {
        return firstEntry().map(Map.Entry::getValue);
    }

    /**
     * @return the base offset of the log segment associated with the smallest offset, if it exists
     */
    protected Optional<Long> firstSegmentBaseOffset() {
        return firstSegment().map(LogSegment::getBaseOffset);
    }

    /**
     * @return the entry associated with the greatest offset, if it exists.
     */
//    @threadsafe
    public Optional<Map.Entry<Long, LogSegment>> lastEntry() {
        return Optional.ofNullable(segments.lastEntry());
    }

    /**
     * @return the log segment with the greatest offset, if it exists.
     */
//    @threadsafe
    public Optional<LogSegment> lastSegment() {
        return lastEntry().map(Map.Entry::getValue);
    }

    /**
     * @return an iterable with log segments ordered from lowest base offset to highest,
     * each segment returned has a base offset strictly greater than the provided baseOffset.
     */
    public Iterable<LogSegment> higherSegments(Long baseOffset) {
        ConcurrentNavigableMap<Long, LogSegment> view = Optional.ofNullable(segments.higherKey(baseOffset))
                .map(higherOffset -> segments.tailMap(higherOffset, true))
                .orElseGet(ConcurrentSkipListMap::new);
        return view.values();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return lastSegment().get();
    }

    public Long sizeInBytes() {
        return LogSegments.sizeInBytes(values());
    }

    /**
     * Returns an Iterable containing segments matching the provided predicate.
     *
     * @param predicate the predicate to be used for filtering segments.
     */
    public List<LogSegment> filter(Predicate<LogSegment> predicate) {
        return values().stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * Calculate a log's size (in bytes) from the provided log segments.
     *
     * @param segments The log segments to calculate the size of
     * @return Sum of the log segments' sizes (in bytes)
     */
    public static Long sizeInBytes(Collection<LogSegment> segments) {
        return segments.stream().mapToLong(LogSegment::size).sum();
    }

    public static List<Long> getFirstBatchTimestampForSegments(Iterable<LogSegment> segments) {
        List<Long> timestamps = new ArrayList<>();
        for (LogSegment seg : segments) {
            timestamps.add(seg.getFirstBatchTimestamp());
        }
        return timestamps;
    }
}
