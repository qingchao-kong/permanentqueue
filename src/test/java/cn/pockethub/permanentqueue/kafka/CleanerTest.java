package cn.pockethub.permanentqueue.kafka;

/**
 * Unit tests for the log cleaning logic
 */
public class CleanerTest {
//    private MockTime time;
//    private Throttler throttler;
//    private File dir;
//    private LogConfigBuilder logConfigBuilder;
//
//    @BeforeEach
//    public void setUp() throws Exception {
//        dir = TestUtils.tempDir();
//        logConfigBuilder = new LogConfigBuilder().segmentSize(1024).maxIndexSize(1024).compact(true);
//        time = new MockTime();
//        throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE, true, time);
//    }
//
//    @AfterEach
//    public void tearDown() throws Exception {
//        Utils.rm(dir);
//    }
//
//    /**
//     * Test simple log cleaning
//     */
//    @Test
//    public void testCleanSegments() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetMapException, LogCleaningAbortedException {
//        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
//            @Override
//            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
//                // ignore
//            }
//        });
//        final Log log = makeLog(dir, logConfigBuilder.segmentSize(1024).build());
//
//        // append messages to the log until we have four segments
//        while (log.numberOfSegments() < 4) {
//            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
//        }
//        final Iterable<Integer> keysFound = keysInLog(log);
//        final List<Integer> keysExpected = Lists.newArrayList();
//
//        for (int i = 0; i < log.logEndOffset(); i++) {
//            keysExpected.add(i);
//        }
//
//        assertEquals(keysFound, keysExpected);
//
//        // pretend we have the following keys
//        final List<Integer> keys = Lists.newArrayList(1, 3, 5, 7, 9);
//        final OffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);
//
//        for (final int k : keys) {
//            map.put(key(k), Long.MAX_VALUE);
//        }
//
//        // clean the log
//        final Iterator<LogSegment> iterator = log.logSegments().iterator();
//        final List<LogSegment> logSegmentsToClean = Lists.newArrayList(iterator.next(), iterator.next(), iterator.next());
//        cleaner.cleanSegments(log, logSegmentsToClean, map, 0L);
//
//        final Iterable<Integer> shouldRemain = Iterables.filter(keysInLog(log), new Predicate<Integer>() {
//            @Override
//            public boolean apply(Integer i) {
//                return !keys.contains(i);
//            }
//        });
//
//        assertEquals(keysInLog(log), shouldRemain);
//    }
//
//    @Test
//    public void testCleaningWithDeletes() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, LogCleaningAbortedException, OffsetMapException {
//        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
//            @Override
//            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
//                // ignore
//            }
//        });
//        final Log log = makeLog(dir, logConfigBuilder.segmentSize(1024).build());
//
//        // append messages with the keys 0 through N
//        while (log.numberOfSegments() < 2) {
//            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
//        }
//
//        // delete all even keys between 0 and N
//        final long leo = log.logEndOffset();
//        for (int i = 0; i < (int) leo; i += 2) {
//            log.append(deleteMessage(i));
//        }
//
//        // append some new unique keys to pad out to a new active segment
//        while (log.numberOfSegments() < 4) {
//            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
//        }
//
//        cleaner.clean(new LogToClean(new TopicAndPartition("test", 0), log, 0));
//
//        final Set<Integer> keys = Sets.newHashSet(keysInLog(log));
//
//        // None of the keys we deleted should still exist.
//        for (int i = 0; i < (int) leo; i += 2) {
//            assertFalse(keys.contains(i));
//        }
//    }
//
//    /* extract all the keys from a log */
//    private Iterable<Integer> keysInLog(final Log log) throws UnsupportedEncodingException {
//        final List<Integer> list = Lists.newArrayList();
//
//        for (final LogSegment segment : log.logSegments()) {
//            for (MessageAndOffset messageAndOffset : segment.getLog()) {
//                if (messageAndOffset.getMessage().isNull()) {
//                    continue;
//                }
//
//                list.add(Integer.parseInt(Utils.readString(messageAndOffset.getMessage().key())));
//            }
//        }
//
//        return list;
//    }
//
//    public void abortCheckDone(final TopicAndPartition topicAndPartition) throws LogCleaningAbortedException {
//        throw new LogCleaningAbortedException();
//    }
//
//    /**
//     * Test that abortion during cleaning throws a LogCleaningAbortedException
//     */
//    @Test
//    public void testCleanSegmentsWithAbort() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetMapException, LogCleaningAbortedException {
//        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
//            @Override
//            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
//                abortCheckDone(argument);
//            }
//        });
//        final Log log = makeLog(dir, logConfigBuilder.segmentSize(1024).build());
//
//        // append messages to the log until we have four segments
//        while (log.numberOfSegments() < 4) {
//            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
//        }
//
//        final Iterable<Integer> keys = keysInLog(log);
//        final OffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);
//
//        for (final int k : keys) {
//            map.put(key(k), Long.MAX_VALUE);
//        }
//
//        final Iterator<LogSegment> iterator = log.logSegments().iterator();
//        final List<LogSegment> logSegmentsToClean = Lists.newArrayList(iterator.next(), iterator.next(), iterator.next());
//
//        assertThrows(LogCleaningAbortedException.class,
//                () -> cleaner.cleanSegments(log, logSegmentsToClean, map, 0L));
//    }
//
//    /**
//     * Validate the logic for grouping log segments together for cleaning
//     */
//    @Test
//    public void testSegmentGrouping() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
//            @Override
//            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
//                // ignore
//            }
//        });
//        final Log log = makeLog(dir, logConfigBuilder.segmentSize(300).indexInterval(1).build());
//
//        int i = 0;
//        while (log.numberOfSegments() < 10) {
//            log.append(TestUtils.singleMessageSet("hello".getBytes()));
//            i += 1;
//        }
//
//        // grouping by very large values should result in a single group with all the segments in it
//        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE);
//        assertEquals(groups.size(), 1);
//        assertEquals(groups.get(0).size(), log.numberOfSegments());
//        checkSegmentOrder(groups);
//
//        // grouping by very small values should result in all groups having one entry
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), 1, Integer.MAX_VALUE);
//        assertEquals(groups.size(), log.numberOfSegments());
//        assertTrue(Iterables.all(groups, new Predicate<List<LogSegment>>() {
//            @Override
//            public boolean apply(List<LogSegment> list) {
//                return list.size() == 1;
//            }
//        }), "All groups should be singletons");
//        checkSegmentOrder(groups);
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, 1);
//        assertEquals(groups.size(), log.numberOfSegments());
//        assertTrue(Iterables.all(groups, new Predicate<List<LogSegment>>() {
//            @Override
//            public boolean apply(List<LogSegment> list) {
//                return list.size() == 1;
//            }
//        }), "All groups should be singletons.");
//        checkSegmentOrder(groups);
//
//        final int groupSize = 3;
//
//        // check grouping by log size
//        final List<LogSegment> logSegments = Lists.newArrayList(log.logSegments());
//        int logSize = 0;
//        for (int idx = 0; idx < groupSize; idx++) {
//            logSize += logSegments.get(idx).size();
//        }
//
//        logSize += 1;
//
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), logSize, Integer.MAX_VALUE);
//        checkSegmentOrder(groups);
//        assertTrue(Iterables.all(groups.subList(0, groups.size() - 1), new Predicate<List<LogSegment>>() {
//            @Override
//            public boolean apply(List<LogSegment> list) {
//                return list.size() == groupSize;
//            }
//        }), "All but the last group should be the target size.");
//
//        // check grouping by index size
//        final List<LogSegment> logSegments2 = Lists.newArrayList(log.logSegments());
//        int indexSize = 0;
//        for (int idx2 = 0; idx2 < groupSize; idx2++) {
//            indexSize += logSegments2.get(idx2).getIndex().sizeInBytes();
//        }
//
//        indexSize += 1;
//
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, indexSize);
//        checkSegmentOrder(groups);
//        assertTrue(Iterables.all(groups.subList(0, groups.size() - 1), new Predicate<List<LogSegment>>() {
//            @Override
//            public boolean apply(List<LogSegment> list) {
//                return list.size() == groupSize;
//            }
//        }), "All but the last group should be the target size.");
//
//    }
//
//    private void checkSegmentOrder(final List<List<LogSegment>> groups) {
//        final List<Long> offsets = Lists.newArrayList();
//        final List<Long> sortedOffsets = Lists.newArrayList();
//
//        for (final List<LogSegment> group : groups) {
//            for (final LogSegment segment : group) {
//                offsets.add(segment.getBaseOffset());
//                sortedOffsets.add(segment.getBaseOffset());
//            }
//
//        }
//
//        Collections.sort(sortedOffsets);
//
//        assertEquals(offsets, sortedOffsets, "Offsets should be in increasing order.");
//    }
//
//    @Test
//    public void testBuildOffsetMap() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetMapException, LogCleaningAbortedException {
//        final FakeOffsetMap map = new FakeOffsetMap(1000);
//        final Log log = makeLog(dir, logConfigBuilder.build());
//        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
//            @Override
//            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
//                // ignore
//            }
//        });
//        int start = 0;
//        int end = 500;
//        final List<Pair<Integer, Integer>> pairs = Lists.newArrayList();
//        for (int i = start; i < end; i++) {
//            pairs.add(ImmutablePair.of(i, i));
//        }
//
//        final Iterable<Long> offsets = writeToLog(log, pairs);
//        final List<LogSegment> segments = Lists.newArrayList(log.logSegments());
//        checkRange(log, cleaner, map, 0, (int) segments.get(1).getBaseOffset());
//        checkRange(log, cleaner, map, (int) segments.get(1).getBaseOffset(), (int) segments.get(3).getBaseOffset());
//        checkRange(log, cleaner, map, (int) segments.get(3).getBaseOffset(), (int) log.logEndOffset());
//
//    }
//
//    public void checkRange(final Log log, final Cleaner cleaner, final FakeOffsetMap map, final int start, final int end) throws LogCleaningAbortedException, IOException, OffsetMapException {
//        final long endOffset = cleaner.buildOffsetMap(log, start, end, map) + 1;
//        assertEquals(endOffset, end, "Last offset should be the end offset.");
//        assertEquals(map.getSize(), end - start, "Should have the expected number of messages in the map.");
//        for (int i = start; i < end; i++) {
//            assertEquals(map.get(key(i)), (long) i, "Should find all the keys");
//        }
//        assertEquals(map.get(key(start - 1)), -1L, "Should not find a value too small");
//        assertEquals(map.get(key(end)), -1L, "Should not find a value too large");
//    }
//
//    public Log makeLog(final File dir, final LogConfig config) throws IOException {
//        return new Log(dir, config, 0L, time.scheduler, time);
//    }
//
//    private Cleaner makeCleaner(final int capacity, final CheckDoneCallback<TopicAndPartition> checkDone) {
//        return new Cleaner(0,
//                new FakeOffsetMap(capacity),
//                64 * 1024,
//                64 * 1024,
//                0.75,
//                throttler,
//                time, checkDone);
//    }
//
//    private Iterable<Long> writeToLog(final Log log, final Iterable<Pair<Integer, Integer>> seq) throws IOException, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final List<Long> list = Lists.newArrayList();
//
//        for (final Pair<Integer, Integer> pair : seq) {
//            list.add(log.append(message(pair.getLeft(), pair.getRight())).firstOffset);
//        }
//
//        return list;
//    }
//
//    public ByteBuffer key(final int id) {
//        return ByteBuffer.wrap(String.valueOf(id).getBytes());
//    }
//
//    public ByteBufferMessageSet message(final int key, final int value) throws IOException {
//        return new ByteBufferMessageSet(
//                Lists.newArrayList(
//                        new Message(String.valueOf(key).getBytes(), String.valueOf(value).getBytes())));
//    }
//
//    private ByteBufferMessageSet deleteMessage(final int key) throws IOException {
//        return new ByteBufferMessageSet(Lists.newArrayList(new Message(null, String.valueOf(key).getBytes())));
//    }
//
//    private class FakeOffsetMap implements OffsetMap {
//        private final Map<String, Long> map = Maps.newHashMap();
//
//        public FakeOffsetMap(int capacity) {
//        }
//
//        private String keyFor(final ByteBuffer key) {
//            return new String(Utils.readBytes(key.duplicate()), Charsets.UTF_8);
//        }
//
//        @Override
//        public int getSlots() {
//            return 0;
//        }
//
//        @Override
//        public void put(ByteBuffer key, long offset) throws OffsetMapException {
//            map.put(keyFor(key), offset);
//        }
//
//        @Override
//        public long get(ByteBuffer key) throws OffsetMapException {
//            final String k = keyFor(key);
//
//            return map.containsKey(k) ? map.get(k) : -1L;
//        }
//
//        @Override
//        public void clear() {
//            map.clear();
//        }
//
//        @Override
//        public int getSize() {
//            return map.size();
//        }
//
//        @Override
//        public double getUtilization() {
//            return 0;
//        }
//    }
}
