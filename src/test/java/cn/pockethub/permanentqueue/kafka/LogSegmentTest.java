package cn.pockethub.permanentqueue.kafka;

public class LogSegmentTest {
//    private List<LogSegment> segments = Lists.newArrayList();
//
//    /* create a segment with the given base offset */
//    private LogSegment createSegment(final long offset) throws IOException {
//        final File msFile = TestUtils.tempFile();
//        final FileMessageSet ms = new FileMessageSet(msFile);
//        final File idxFile = TestUtils.tempFile();
//        idxFile.delete();
//        final OffsetIndex idx = new OffsetIndex(idxFile, offset, 1000);
//        final LogSegment seg = new LogSegment(ms, idx, offset, 10, 0, new SystemTime());
//        segments.add(seg);
//        return seg;
//    }
//
//    /* create a ByteBufferMessageSet for the given messages starting from the given offset */
//    private ByteBufferMessageSet messages(final long offset, final String... messages) throws IOException {
//        return new ByteBufferMessageSet(CompressionCodec.NONE, new AtomicLong(offset), Lists.newArrayList(
//                Iterables.transform(Lists.newArrayList(messages), new Function<String, Message>() {
//                    @Override
//                    public Message apply(String msg) {
//                        return new Message(msg.getBytes());
//                    }
//                })));
//    }
//
//    @AfterEach
//    public void tearDown() throws Exception {
//        for (final LogSegment segment : segments) {
//            segment.getIndex().delete();
//            segment.getLog().delete();
//        }
//        segments.clear();
//    }
//
//    /**
//     * A read on an empty log segment should return null
//     */
//    @Test
//    public void testReadOnEmptySegment() throws Exception {
//        final LogSegment seg = createSegment(40);
//        final FetchDataInfo read = seg.read(40, Optional.<Long>absent(), 300);
//        assertNull(read, "Read beyond the last offset in the segment should be null");
//    }
//
//    /**
//     * Reading from before the first offset in the segment should return messages
//     * beginning with the first message in the segment
//     */
//    @Test
//    public void testReadBeforeFirstOffset() throws Exception {
//        final LogSegment seg = createSegment(40);
//        final ByteBufferMessageSet ms = messages(50, "hello", "there", "little", "bee");
//        seg.append(50, ms);
//        final MessageSet read = seg.read(41, Optional.<Long>absent(), 300).getMessageSet();
//        assertEquals(Lists.newArrayList(read), Lists.newArrayList(ms));
//    }
//
//    /**
//     * If we set the startOffset and maxOffset for the read to be the same value
//     * we should get only the first message in the log
//     */
//    @Test
//    public void testMaxOffset() throws Exception {
//        final long baseOffset = 50L;
//        final LogSegment seg = createSegment(baseOffset);
//        final ByteBufferMessageSet ms = messages(baseOffset, "hello", "there", "beautiful");
//        seg.append(baseOffset, ms);
//
//        validate(ms, seg, 50);
//        validate(ms, seg, 51);
//        validate(ms, seg, 52);
//    }
//
//    private void validate(final ByteBufferMessageSet messages, final LogSegment seg, final long offset) throws IOException {
//        final List<MessageAndOffset> filtered = Lists.newArrayList(Iterables.filter(messages, new Predicate<MessageAndOffset>() {
//            @Override
//            public boolean apply(MessageAndOffset messageAndOffset) {
//                return messageAndOffset.getOffset() == offset;
//            }
//        }));
//        assertEquals(Lists.newArrayList(seg.read(offset, Optional.of(offset + 1), 1024).getMessageSet()), filtered);
//    }
//
//    /**
//     * If we read from an offset beyond the last offset in the segment we should get null
//     */
//    @Test
//    public void testReadAfterLast() throws Exception {
//        final LogSegment seg = createSegment(40);
//        final ByteBufferMessageSet ms = messages(50, "hello", "there");
//        seg.append(50, ms);
//        final FetchDataInfo read = seg.read(52, Optional.<Long>absent(), 200);
//        assertNull(read, "Read beyond the last offset in the segment should give null");
//    }
//
//    /**
//     * If we read from an offset which doesn't exist we should get a message set beginning
//     * with the least offset greater than the given startOffset.
//     */
//    @Test
//    public void testReadFromGap() throws Exception {
//        final LogSegment seg = createSegment(40);
//        final ByteBufferMessageSet ms = messages(50, "hello", "there");
//        seg.append(50, ms);
//        final ByteBufferMessageSet ms2 = messages(60, "alpha", "beta");
//        seg.append(60, ms2);
//        final FetchDataInfo read = seg.read(55, Optional.<Long>absent(), 200);
//        assertEquals(Lists.newArrayList(read.getMessageSet()), Lists.newArrayList(ms2));
//    }
//
//    /**
//     * In a loop append two messages then truncate off the second of those messages and check that we can read
//     * the first but not the second message.
//     */
//    @Test
//    public void testTruncate() throws Exception {
//        final LogSegment seg = createSegment(40);
//        long offset = 40;
//        for (int i = 0; i < 30; i++) {
//            final ByteBufferMessageSet ms1 = messages(offset, "hello");
//            seg.append(offset, ms1);
//            final ByteBufferMessageSet ms2 = messages(offset + 1, "hello");
//            seg.append(offset + 1, ms2);
//            // check that we can read back both messages
//            final FetchDataInfo read = seg.read(offset, Optional.<Long>absent(), 10000);
//            assertEquals(Lists.newArrayList(read.getMessageSet()),
//                    Lists.newArrayList(Iterables.getFirst(ms1, null), Iterables.getFirst(ms2, null)));
//            // now truncate off the last message
//            seg.truncateTo(offset + 1);
//            final FetchDataInfo read2 = seg.read(offset, Optional.<Long>absent(), 10000);
//            assertEquals(Iterables.size(read2.getMessageSet()), 1);
//            assertEquals(Iterables.getFirst(read2.getMessageSet(), null), Iterables.getFirst(ms1, null));
//            offset += 1;
//        }
//    }
//
//    /**
//     * Test truncating the whole segment, and check that we can reappend with the original offset.
//     */
//    @Test
//    public void testTruncateFull() throws Exception {
//        // test the case where we fully truncate the log
//        final LogSegment seg = createSegment(40);
//        seg.append(40, messages(40, "hello", "there"));
//        seg.truncateTo(0);
//        assertNull(seg.read(0, Optional.<Long>absent(), 1024), "Segment should be empty.");
//        seg.append(40, messages(40, "hello", "there"));
//    }
//
//    /**
//     * Test that offsets are assigned sequentially and that the nextOffset variable is incremented
//     */
//    @Test
//    public void testNextOffsetCalculation() throws Exception {
//        final LogSegment seg = createSegment(40);
//        assertEquals(40, seg.nextOffset());
//        seg.append(50, messages(50, "hello", "there", "you"));
//        assertEquals(53, seg.nextOffset());
//    }
//
//    /**
//     * Test that we can change the file suffixes for the log and index files
//     */
//    @Test
//    public void testChangeFileSuffixes() throws Exception, SamsaStorageException {
//        final LogSegment seg = createSegment(40);
//        final File logFile = seg.getLog().getFile();
//        final File indexFile = seg.getIndex().getFile();
//        seg.changeFileSuffixes("", ".deleted");
//        assertEquals(seg.getLog().getFile().getAbsolutePath(), logFile.getAbsolutePath() + ".deleted");
//        assertEquals(seg.getIndex().getFile().getAbsolutePath(), indexFile.getAbsolutePath() + ".deleted");
//        assertTrue(seg.getLog().getFile().exists());
//        assertTrue(seg.getIndex().getFile().exists());
//    }
//
//    /**
//     * Create a segment with some data and an index. Then corrupt the index,
//     * and recover the segment, the entries should all be readable.
//     */
//    @Test
//    public void testRecoveryFixesCorruptIndex() throws Exception {
//        final LogSegment seg = createSegment(0);
//        for (int i = 0; i < 100; i++) {
//            seg.append(i, messages(i, String.valueOf(i)));
//        }
//        final File indexFile = seg.getIndex().getFile();
//        TestUtils.writeNonsenseToFile(indexFile, 5, (int) indexFile.length());
//        seg.recover(64 * 1024);
//        for (int i = 0; i < 100; i++) {
//            assertEquals(Iterables.getFirst(seg.read(i, Optional.of(i + 1L), 1024).getMessageSet(), null).getOffset(), i);
//        }
//    }
//
//    /**
//     * Randomly corrupt a log a number of times and attempt recovery.
//     */
//    @Test
//    public void testRecoveryWithCorruptMessage() throws Exception, SamsaStorageException {
//        final int messagesAppended = 20;
//        for (int iteration = 0; iteration < 10; iteration++) {
//            final LogSegment seg = createSegment(0);
//            for (int i = 0; i < messagesAppended; i++) {
//                seg.append(i, messages(i, String.valueOf(i)));
//            }
//            final int offsetToBeginCorruption = TestUtils.RANDOM.nextInt(messagesAppended);
//            // start corrupting somewhere in the middle of the chosen record all the way to the end
//            final int position = seg.getLog().searchFor(offsetToBeginCorruption, 0).getPosition() + TestUtils.RANDOM.nextInt(15);
//            TestUtils.writeNonsenseToFile(seg.getLog().getFile(), position, ((int) seg.getLog().getFile().length()) - position);
//            seg.recover(64 * 1024);
//            final List<Long> offsets = Lists.newArrayList();
//            final List<Long> expectedOffsets = Lists.newArrayList();
//            for (final MessageAndOffset messageAndOffset : seg.getLog()) {
//                offsets.add(messageAndOffset.getOffset());
//            }
//            for (long i = 0L; i < offsetToBeginCorruption; i++) {
//                expectedOffsets.add(i);
//            }
//
//            assertEquals(offsets, expectedOffsets, "Should have truncated off bad messages.");
//            seg.delete();
//        }
//    }
}
