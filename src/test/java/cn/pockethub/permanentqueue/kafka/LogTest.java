package cn.pockethub.permanentqueue.kafka;

public class LogTest {
//    private File logDir;
//    private MockTime time;
//    private LogConfigBuilder logConfigBuilder;
//
//    @BeforeEach
//    public void setUp() throws Exception {
//        logDir = TestUtils.tempDir();
//        time = new MockTime(0);
//        logConfigBuilder = new LogConfigBuilder();
//    }
//
//    @AfterEach
//    public void tearDown() throws Exception {
//        Utils.rm(logDir);
//    }
//
//    private void createEmptyLogs(final File dir, final int... offsets) throws IOException {
//        for (final int offset : offsets) {
//            Log.logFilename(dir, offset).createNewFile();
//            Log.indexFilename(dir, offset).createNewFile();
//        }
//    }
//
//    /**
//     * Tests for time based log roll. This test appends messages then changes the time
//     * using the mock clock to force the log to roll and checks the number of segments.
//     */
//    @Test
//    public void testTimeBasedLogRoll() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//
//        // create a log
//        final Log log = new Log(logDir,
//                logConfigBuilder.segmentMs(1 * 60 * 60L).build(),
//                0L,
//                time.scheduler,
//                time);
//        assertEquals(log.numberOfSegments(), 1, "Log begins with a single empty segment.");
//        time.sleep(log.getConfig().getSegmentMs() + 1);
//        log.append(set);
//        assertEquals(log.numberOfSegments(), 1, "Log doesn't roll if doing so creates an empty segment.");
//
//        log.append(set);
//        assertEquals(log.numberOfSegments(), 2, "Log rolls on this append since time has expired.");
//
//        for (int numSegments = 3; numSegments < 5; numSegments++) {
//            time.sleep(log.getConfig().getSegmentMs() + 1);
//            log.append(set);
//            assertEquals(log.numberOfSegments(), numSegments, "Changing time beyond rollMs and appending should create a new segment.");
//        }
//
//        final int numSegments = log.numberOfSegments();
//        time.sleep(log.getConfig().getSegmentMs() + 1);
//        log.append(new ByteBufferMessageSet(new ArrayList<Message>()));
//        assertEquals(log.numberOfSegments(), numSegments, "Appending an empty message set should not roll log even if succient time has passed.");
//    }
//
//    /**
//     * Test for jitter s for time based log roll. This test appends messages then changes the time
//     * using the mock clock to force the log to roll and checks the number of segments.
//     */
//    @Test
//    public void testTimeBasedLogRollJitter() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final long maxJitter = 20 * 60L;
//
//        // create a log
//        final Log log = new Log(logDir,
//                logConfigBuilder.segmentMs(1 * 60 * 60L).segmentJitterMs(maxJitter).build(),
//                0L,
//                time.scheduler,
//                time);
//        assertEquals(log.numberOfSegments(), 1, "Log begins with a single empty segment.");
//        log.append(set);
//
//        time.sleep(log.getConfig().getSegmentMs() - maxJitter);
//        log.append(set);
//        assertEquals(log.numberOfSegments(), 1, "Log does not roll on this append because it occurs earlier than max jitter");
//        time.sleep(maxJitter - log.activeSegment().getRollJitterMs() + 1);
//        log.append(set);
//        assertEquals(log.numberOfSegments(), 2, "Log should roll after segmentMs adjusted by random jitter");
//    }
//
//    /**
//     * Test that appending more than the maximum segment size rolls the log
//     */
//    @Test
//    public void testSizeBasedLogRoll() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final int setSize = set.sizeInBytes();
//        final int msgPerSeg = 10;
//        final int segmentSize = msgPerSeg * (setSize - 1); // each segment will be 10 messages
//
//        // create a log
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(segmentSize).build(), 0L, time.scheduler, time);
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segment.");
//
//        // segments expire in size
//        for (int i = 1; i < (msgPerSeg + 1); i++) {
//            log.append(set);
//        }
//        assertEquals(log.numberOfSegments(), 2, "There should be exactly 2 segments.");
//    }
//
//    /**
//     * Test that we can open and append to an empty log
//     */
//    @Test
//    public void testLoadEmptyLog() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        createEmptyLogs(logDir, 0);
//        final Log log = new Log(logDir, logConfigBuilder.build(), 0L, time.scheduler, time);
//        log.append(TestUtils.singleMessageSet("test".getBytes()));
//    }
//
//    /**
//     * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
//     */
//    @Test
//    public void testAppendAndReadWithSequentialOffsets() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(71).build(), 0L, time.scheduler, time);
//        final List<Message> messages = Lists.newArrayList();
//        for (int i = 0; i < 100; i += 2) {
//            messages.add(new Message(String.valueOf(i).getBytes()));
//        }
//
//        for (int i = 0; i < messages.size(); i++) {
//            log.append(new ByteBufferMessageSet(CompressionCodec.NONE, Lists.newArrayList(messages.get(i))));
//        }
//        for (int i = 0; i < messages.size(); i++) {
//            final MessageAndOffset read = Iterables.getFirst(log.read(i, 100, Optional.of(i + 1L)).getMessageSet(), null);
//            assertEquals(read.getOffset(), i, "Offset read should match order appended.");
//            assertEquals(read.getMessage(), messages.get(i), "Message should match appended.");
//        }
//        assertEquals(Iterables.size(log.read(messages.size(), 100, Optional.<Long>absent()).getMessageSet()), 0, "Reading beyond the last message returns nothing.");
//    }
//
//    /**
//     * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
//     * from any offset less than the logEndOffset including offsets not appended.
//     */
//    @Test
//    public void testAppendAndReadWithNonSequentialOffsets() throws Exception, SamsaStorageException, MessageSetSizeTooLargeException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(71).build(), 0L, time.scheduler, time);
//        final List<Integer> messageIds = Lists.newArrayList();
//        final List<Message> messages = Lists.newArrayList();
//
//        for (int i = 0; i < 50; i++) {
//            messageIds.add(i);
//        }
//        for (int i = 50; i < 200; i += 7) {
//            messageIds.add(i);
//        }
//
//        for (final int messageId : messageIds) {
//            messages.add(new Message(String.valueOf(messageId).getBytes()));
//        }
//
//        // now test the case that we give the offsets and use non-sequential offsets
//        for (int i = 0; i < messages.size(); i++) {
//            log.append(new ByteBufferMessageSet(CompressionCodec.NONE,
//                            new AtomicLong(messageIds.get(i)),
//                            Lists.newArrayList(messages.get(i))),
//                    false);
//        }
//        for (int i = 50; i < Collections.max(messageIds); i++) {
//            final int num = i;
//            final int idx = Iterables.indexOf(messageIds, new Predicate<Integer>() {
//                @Override
//                public boolean apply(Integer id) {
//                    return id >= num;
//                }
//            });
//            final MessageAndOffset read = Iterables.getFirst(log.read(i, 100, Optional.<Long>absent()).getMessageSet(), null);
//            assertEquals(read.getOffset(), (long) messageIds.get(idx), "Offset read should match message id.");
//            assertEquals(read.getMessage(), messages.get(idx), "Message should match appended.");
//        }
//    }
//
//    /**
//     * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
//     * Specifically we create a log where the last message in the first segment has offset 0. If we
//     * then read offset 1, we should expect this read to come from the second segment, even though the
//     * first segment has the greatest lower bound on the offset.
//     */
//    @Test
//    public void testReadAtLogGap() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(300).build(), 0L, time.scheduler, time);
//
//        // keep appending until we have two segments with only a single message in the second segment
//        while (log.numberOfSegments() == 1) {
//            log.append(new ByteBufferMessageSet(CompressionCodec.NONE, Lists.newArrayList(new Message("42".getBytes()))));
//        }
//
//        // now manually truncate off all but one message from the first segment to create a gap in the messages
//        Iterables.getFirst(log.logSegments(), null).truncateTo(1);
//
//        assertEquals(Iterables.getFirst(log.read(1, 200, Optional.<Long>absent()).getMessageSet(), null).getOffset(),
//                log.logEndOffset() - 1, "A read should now return the last message in the log");
//    }
//
//    /**
//     * Test reading at the boundary of the log, specifically
//     * - reading from the logEndOffset should give an empty message set
//     * - reading beyond the log end offset should throw an OffsetOutOfRangeException
//     */
//    @Test
//    public void testReadOutOfRange() throws Exception, OffsetOutOfRangeException {
//        createEmptyLogs(logDir, 1024);
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(1024).build(), 0L, time.scheduler, time);
//        assertEquals(log.read(1024, 1000).getMessageSet().sizeInBytes(), 0, "Reading just beyond end of log should produce 0 byte read.");
//        try {
//            log.read(0, 1024);
//            assertTrue(false, "Expected exception on invalid read.");
//        } catch (OffsetOutOfRangeException e) {
//            // This is good.
//        }
//        try {
//            log.read(1025, 1000);
//            assertTrue(false, "Expected exception on invalid read.");
//        } catch (OffsetOutOfRangeException e) {
//            // This is good.
//        }
//    }
//
//    /**
//     * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
//     * and then reads them all back and checks that the message read and offset matches what was appended.
//     */
//    @Test
//    public void testLogRolls() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        /* create a multipart log with 100 messages */
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(100).build(), 0L, time.scheduler, time);
//        final int numMessages = 100;
//        final List<ByteBufferMessageSet> messageSets = Lists.newArrayList();
//        for (int i = 0; i < numMessages; i++) {
//            messageSets.add(TestUtils.singleMessageSet(String.valueOf(i).getBytes()));
//        }
//        for (final ByteBufferMessageSet messageSet : messageSets) {
//            log.append(messageSet);
//        }
//        log.flush();
//
//        /* do successive reads to ensure all our messages are there */
//        long offset = 0L;
//        for (int i = 0; i < numMessages; i++) {
//            final MessageSet messages = log.read(offset, 1024 * 1024).getMessageSet();
//            assertEquals(Iterables.get(messages, 0).getOffset(), offset, "Offsets not equal");
//            assertEquals(Iterables.get(messages, 0).getMessage(),
//                    Iterables.get(messageSets.get(i), 0).getMessage(),
//                    "Messages not equal at offset " + offset);
//            offset = Iterables.get(messages, 0).getOffset() + 1;
//        }
//        final MessageSet lastRead = log.read(numMessages, 1024 + 1024, Optional.of(numMessages + 1L)).getMessageSet();
//        assertEquals(Iterables.size(lastRead), 0, "Should be no more messages");
//
//        // check that rolling the log forced a flushed the log--the flush is async so retry in case of failure
//        TestUtils.retry(1000L, new Runnable() {
//            @Override
//            public void run() {
//                assertTrue(log.getRecoveryPoint() >= log.activeSegment().getBaseOffset(), "Log role should have forced flush");
//            }
//        });
//    }
//
//    /**
//     * Test reads at offsets that fall within compressed message set boundaries.
//     */
//    @Test
//    public void testCompressedMessages() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        /* this log should roll after every messageset */
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(100).build(), 0L, time.scheduler, time);
//
//        /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
//        log.append(new ByteBufferMessageSet(CompressionCodec.GZIP,
//                Lists.newArrayList(new Message("hello".getBytes()), new Message("there".getBytes()))));
//        log.append(new ByteBufferMessageSet(CompressionCodec.GZIP,
//                Lists.newArrayList(new Message("alpha".getBytes()), new Message("beta".getBytes()))));
//
//
//        /* we should always get the first message in the compressed set when reading any offset in the set */
//        assertEquals(Iterables.get(decompressRead(log, 0), 0).getOffset(), 0, "Read at offset 0 should produce 0");
//        assertEquals(Iterables.get(decompressRead(log, 1), 0).getOffset(), 0, "Read at offset 1 should produce 0");
//        assertEquals(Iterables.get(decompressRead(log, 2), 0).getOffset(), 2, "Read at offset 2 should produce 0");
//        assertEquals(Iterables.get(decompressRead(log, 3), 0).getOffset(), 2, "Read at offset 3 should produce 0");
//    }
//
//    private ByteBufferMessageSet decompressRead(final Log log, final int offset) throws IOException, OffsetOutOfRangeException {
//        return ByteBufferMessageSet.decompress(Iterables.get(log.read(offset, 4096).getMessageSet(), 0).getMessage());
//    }
//
//    /**
//     * Test garbage collecting old segments
//     */
//    @Test
//    public void testThatGarbageCollectingSegmentsDoesntChangeOffset() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        for (final int messagesToAppend : Lists.newArrayList(0, 1, 25)) {
//            logDir.mkdirs();
//            // first test a log segment starting at 0
//            final Log log = new Log(logDir, logConfigBuilder.segmentSize(100).build(), 0L, time.scheduler, time);
//            for (int i = 0; i < messagesToAppend; i++) {
//                log.append(TestUtils.singleMessageSet(String.valueOf(i).getBytes()));
//            }
//
//            final long currOffset = log.logEndOffset();
//            assertEquals(messagesToAppend, currOffset);
//
//            // time goes by; the log file is deleted
//            log.deleteOldSegments(new Function<LogSegment, Boolean>() {
//                @Override
//                public Boolean apply(LogSegment input) {
//                    return true;
//                }
//            });
//
//            assertEquals(log.logEndOffset(), currOffset, "Deleting segments shouldn't have changed the logEndOffset");
//            assertEquals(log.numberOfSegments(), 1, "We should still have one segment left");
//            assertEquals(log.deleteOldSegments(new Function<LogSegment, Boolean>() {
//                @Override
//                public Boolean apply(LogSegment input) {
//                    return true;
//                }
//            }), 0, "Further collection shouldn't delete anything");
//            assertEquals(log.logEndOffset(), currOffset, "Still no change in the logEndOffset");
//            assertEquals(log.append(TestUtils.singleMessageSet("hello".getBytes())).firstOffset, currOffset,
//                    "Should still be able to append and should get the logEndOffset assigned to the new append");
//
//            // cleanup the log
//            log.delete();
//        }
//    }
//
//    /**
//     * MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
//     * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
//     */
//    @Test
//    public void testMessageSetSizeCheck() throws Exception {
//        final ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.NONE,
//                Lists.newArrayList(new Message("You".getBytes()), new Message("bethe".getBytes())));
//        // append messages to log
//        final int configSegmentSize = messageSet.sizeInBytes() - 1;
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(configSegmentSize).build(), 0L, time.scheduler, time);
//
//        try {
//            log.append(messageSet);
//            assertTrue(false, "message set should throw MessageSetSizeTooLargeException.");
//        } catch (MessageSetSizeTooLargeException e) {
//            // this is good
//        } catch (SamsaStorageException | MessageSizeTooLargeException | InvalidMessageSizeException e) {
//            assertTrue(false, "message set should not throw any of these " + e.getMessage() + " " + e.getClass().getCanonicalName());
//        }
//    }
//
//    /**
//     * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
//     * setting and checking that an exception is thrown.
//     */
//    @Test
//    public void testMessageSizeCheck() throws Exception {
//        final ByteBufferMessageSet first = new ByteBufferMessageSet(CompressionCodec.NONE,
//                Lists.newArrayList(new Message("You".getBytes()), new Message("bethe".getBytes())));
//        final ByteBufferMessageSet second = new ByteBufferMessageSet(CompressionCodec.NONE,
//                Lists.newArrayList(new Message("change".getBytes())));
//
//        // append messages to log
//        final int maxMessageSize = second.sizeInBytes() - 1;
//        final Log log = new Log(logDir, logConfigBuilder.maxMessageSize(maxMessageSize).build(), 0L, time.scheduler, time);
//
//        // should be able to append the small message
//        try {
//            log.append(first);
//        } catch (SamsaStorageException | MessageSetSizeTooLargeException | InvalidMessageSizeException | MessageSizeTooLargeException e) {
//            assertTrue(false, "should not throw any of this: " + e.getMessage() + " " + e.getClass().getCanonicalName());
//        }
//
//        try {
//            log.append(second);
//            assertTrue(false, "Second message set should throw MessageSizeTooLargeException.");
//        } catch (MessageSizeTooLargeException e) {
//            // this is good
//        } catch (MessageSetSizeTooLargeException | SamsaStorageException | InvalidMessageSizeException e) {
//            assertTrue(false, "should not throw any of this " + e.getMessage() + " " + e.getClass().getCanonicalName());
//        }
//    }
//
//    /**
//     * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
//     */
//    @Test
//    public void testLogRecoversToCorrectOffset() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final int numMessages = 100;
//        final int messageSize = 100;
//        final int segmentSize = 7 * messageSize;
//        final int indexInterval = 3 * messageSize;
//        final LogConfig config = logConfigBuilder.segmentSize(segmentSize).indexInterval(indexInterval).maxIndexSize(4096).build();
//        Log log = new Log(logDir, config, 0L, time.scheduler, time);
//        for (int i = 0; i < numMessages; i++) {
//            log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(messageSize)));
//        }
//        assertEquals(log.logEndOffset(), numMessages,
//                String.format("After appending %d messages to an empty log, the log end offset should be %d", numMessages, numMessages));
//        final long lastIndexOffset = log.activeSegment().getIndex().getLastOffset();
//        final int numIndexEntries = log.activeSegment().getIndex().entries();
//        final long lastOffset = log.logEndOffset();
//        log.close();
//
//        log = new Log(logDir, config, lastOffset, time.scheduler, time);
//        assertEquals(log.logEndOffset(), numMessages,
//                String.format("Should have %d messages when log is reopened w/o recovery", numMessages));
//        assertEquals(log.activeSegment().getIndex().getLastOffset(), lastIndexOffset, "Should have same last index offset as before.");
//        assertEquals(log.activeSegment().getIndex().entries(), numIndexEntries, "Should have same number of index entries as before.");
//        log.close();
//
//        // test recovery case
//        log = new Log(logDir, config, 0L, time.scheduler, time);
//        assertEquals(log.logEndOffset(), numMessages,
//                String.format("Should have %d messages when log is reopened with recovery", numMessages));
//        assertEquals(log.activeSegment().getIndex().getLastOffset(), lastIndexOffset, "Should have same last index offset as before.");
//        assertEquals(log.activeSegment().getIndex().entries(), numIndexEntries, "Should have same number of index entries as before.");
//        log.close();
//    }
//
//    /**
//     * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
//     */
//    @Test
//    public void testIndexRebuild() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        // publish the messages and close the log
//        final int numMessages = 200;
//        final LogConfig config = logConfigBuilder.segmentSize(200).indexInterval(1).build();
//        Log log = new Log(logDir, config, 0L, time.scheduler, time);
//        for (int i = 0; i < numMessages; i++) {
//            log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(10)));
//        }
//        final Iterable<File> indexFiles = Iterables.transform(log.logSegments(), new Function<LogSegment, File>() {
//            @Override
//            public File apply(final LogSegment segment) {
//                return segment.getIndex().getFile();
//            }
//        });
//        log.close();
//
//        // delete all the index files
//        for (final File indexFile : indexFiles) {
//            indexFile.delete();
//        }
//
//        // reopen the log
//        log = new Log(logDir, config, 0L, time.scheduler, time);
//        assertEquals(log.logEndOffset(), numMessages, String.format("Should have %d messages when log is reopened", numMessages));
//        for (int i = 0; i < numMessages; i++) {
//            assertEquals(Iterables.get(log.read(i, 100, Optional.<Long>absent()).getMessageSet(), 0).getOffset(), i);
//        }
//        log.close();
//    }
//
//    /**
//     * Test the Log truncate operations
//     */
//    @Test
//    public void testTruncateTo() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final int setSize = set.sizeInBytes();
//        final int msgPerSeg = 10;
//        final int segmentSize = msgPerSeg * setSize;  // each segment will be 10 messages
//
//        // create a log
//        final Log log = new Log(logDir, logConfigBuilder.segmentSize(segmentSize).build(), 0L, time.scheduler, time);
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segment.");
//
//        for (int i = 1; i <= msgPerSeg; i++) {
//            log.append(set);
//        }
//
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segments.");
//        assertEquals(log.logEndOffset(), msgPerSeg, "Log end offset should be equal to number of messages");
//
//        final long lastOffset = log.logEndOffset();
//        final long size = log.size();
//        log.truncateTo(log.logEndOffset()); // keep the entire log
//        assertEquals(log.logEndOffset(), lastOffset, "Should not change offset");
//        assertEquals(log.size(), size, "Should not change log size");
//        log.truncateTo(log.logEndOffset() + 1); // try to truncate beyond lastOffset
//        assertEquals(log.logEndOffset(), lastOffset, "Should not change offset but should log error");
//        assertEquals(log.size(), size, "Should not change log size");
//        log.truncateTo(msgPerSeg / 2); // truncate somewhere in between
//        assertEquals(log.logEndOffset(), msgPerSeg / 2, "Should change offset");
//        assertTrue(log.size() < size, "Should change log size");
//        log.truncateTo(0); // truncate the entire log
//        assertEquals(log.logEndOffset(), 0, "Should change offset");
//        assertEquals(log.size(), 0, "Should change log size");
//
//        for (int i = 1; i <= msgPerSeg; i++) {
//            log.append(set);
//        }
//
//        assertEquals(log.logEndOffset(), lastOffset, "Should be back to original offset");
//        assertEquals(log.size(), size, "Should be back to original size");
//        log.truncateFullyAndStartAt(log.logEndOffset() - (msgPerSeg - 1));
//        assertEquals(log.logEndOffset(), lastOffset - (msgPerSeg - 1), "Should change offset");
//        assertEquals(log.size(), 0, "Should change log size");
//
//        for (int i = 1; i <= msgPerSeg; i++) {
//            log.append(set);
//        }
//
//        assertTrue(log.logEndOffset() > msgPerSeg, "Should be ahead of to original offset");
//        assertEquals(log.size(), size, "log size should be same as before");
//        log.truncateTo(0); // truncate before first start offset in the log
//        assertEquals(log.logEndOffset(), 0, "Should change offset");
//        assertEquals(log.size(), 0, "Should change log size");
//    }
//
//    /**
//     * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
//     */
//    @Test
//    public void testIndexResizingAtTruncation() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final int setSize = set.sizeInBytes();
//        final int msgPerSeg = 10;
//        final int segmentSize = msgPerSeg * setSize;  // each segment will be 10 messages
//        final LogConfig config = logConfigBuilder.segmentSize(segmentSize).build();
//        final Log log = new Log(logDir, config, 0L, time.scheduler, time);
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segment.");
//        for (int i = 1; i <= msgPerSeg; i++) {
//            log.append(set);
//        }
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segment.");
//        for (int i = 1; i <= msgPerSeg; i++) {
//            log.append(set);
//        }
//        assertEquals(log.numberOfSegments(), 2, "There should be exactly 2 segment.");
//        assertEquals(Iterables.get(log.logSegments(), 0).getIndex().getMaxEntries(), 0,
//                "The index of the first segment should be trimmed to empty");
//        log.truncateTo(0);
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segment.");
//        assertEquals(Iterables.get(log.logSegments(), 0).getIndex().getMaxEntries(),
//                log.getConfig().getMaxIndexSize() / 8,
//                "The index of segment 1 should be resized to maxIndexSize");
//        for (int i = 1; i <= msgPerSeg; i++) {
//            log.append(set);
//        }
//        assertEquals(log.numberOfSegments(), 1, "There should be exactly 1 segment.");
//    }
//
//    /**
//     * When we open a log any index segments without an associated log segment should be deleted.
//     */
//    @Test
//    public void testBogusIndexSegmentsAreRemoved() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final File bogusIndex1 = Log.indexFilename(logDir, 0);
//        final File bogusIndex2 = Log.indexFilename(logDir, 5);
//
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final Log log = new Log(logDir,
//                logConfigBuilder.segmentSize(set.sizeInBytes() * 5).maxIndexSize(1000).indexInterval(1).build(),
//                0L, time.scheduler, time);
//
//        assertTrue(bogusIndex1.length() > 0, "The first index file should have been replaced with a larger file");
//        assertFalse(bogusIndex2.exists(), "The second index file should have been deleted.");
//
//        // check that we can append to the log
//        for (int i = 0; i < 10; i++) {
//            log.append(set);
//        }
//
//        log.delete();
//    }
//
//    /**
//     * Verify that truncation works correctly after re-opening the log
//     */
//    @Test
//    public void testReopenThenTruncate() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final LogConfig config = logConfigBuilder.segmentSize(set.sizeInBytes() * 5).maxIndexSize(1000).indexInterval(10000).build();
//
//        // create a log
//        Log log = new Log(logDir, config, 0L, time.scheduler, time);
//
//        // add enough messages to roll over several segments then close and re-open and attempt to truncate
//        for (int i = 0; i < 100; i++) {
//            log.append(set);
//        }
//        log.close();
//        log = new Log(logDir, config, 0L, time.scheduler, time);
//        log.truncateTo(3);
//        assertEquals(log.numberOfSegments(), 1, "All but one segment should be deleted.");
//        assertEquals(log.logEndOffset(), 3, "Log end offset should be 3.");
//    }
//
//    /**
//     * Test that deleted files are deleted after the appropriate time.
//     */
//    @Test
//    public void testAsyncDelete() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final int asyncDeleteMs = 1000;
//        final LogConfig config = logConfigBuilder.segmentSize(set.sizeInBytes() * 5)
//                .fileDeleteDelayMs(asyncDeleteMs).maxIndexSize(1000).indexInterval(10000).build();
//        final Log log = new Log(logDir, config, 0L, time.scheduler, time);
//
//        // append some messages to create some segments
//        for (int i = 0; i < 100; i++) {
//            log.append(set);
//        }
//
//        // files should be renamed
//        final List<LogSegment> segments = Lists.newArrayList(log.logSegments());
//        final List<File> oldFiles = Lists.newArrayList();
//
//        for (final LogSegment segment : log.logSegments()) {
//            oldFiles.add(segment.getLog().getFile());
//            oldFiles.add(segment.getIndex().getFile());
//        }
//
//        log.deleteOldSegments(new Function<LogSegment, Boolean>() {
//            @Override
//            public Boolean apply(LogSegment input) {
//                return true;
//            }
//        });
//
//        assertEquals(log.numberOfSegments(), 1, "Only one segment should remain.");
//
//        assertTrue(Iterables.all(segments, new Predicate<LogSegment>() {
//            @Override
//            public boolean apply(LogSegment segment) {
//                return segment.getLog().getFile().getName().endsWith(Log.DELETED_FILE_SUFFIX) &&
//                        segment.getIndex().getFile().getName().endsWith(Log.DELETED_FILE_SUFFIX);
//            }
//        }), "All log and index files should end in .deleted");
//
//        assertTrue(Iterables.all(segments, new Predicate<LogSegment>() {
//            @Override
//            public boolean apply(LogSegment segment) {
//                return segment.getLog().getFile().exists() && segment.getIndex().getFile().exists();
//            }
//        }), "The .deleted files should still be there.");
//
//        assertTrue(Iterables.all(oldFiles, new Predicate<File>() {
//            @Override
//            public boolean apply(File file) {
//                return !file.exists();
//            }
//        }), "The original file should be gone.");
//
//        // when enough time passes the files should be deleted
//        final List<File> deletedFiles = Lists.newArrayList();
//        for (final LogSegment segment : segments) {
//            deletedFiles.add(segment.getLog().getFile());
//            deletedFiles.add(segment.getIndex().getFile());
//        }
//
//        time.sleep(asyncDeleteMs + 1);
//        assertTrue(Iterables.all(deletedFiles, new Predicate<File>() {
//            @Override
//            public boolean apply(File file) {
//                return !file.exists();
//            }
//        }), "Files should all be gone.");
//    }
//
//    /**
//     * Any files ending in .deleted should be removed when the log is re-opened.
//     */
//    @Test
//    public void testOpenDeletesObsoleteFiles() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final LogConfig config = logConfigBuilder.segmentSize(set.sizeInBytes() * 5).maxIndexSize(1000).build();
//        Log log = new Log(logDir, config, 0L, time.scheduler, time);
//
//        // append some messages to create some segments
//        for (int i = 0; i < 100; i++) {
//            log.append(set);
//        }
//
//        log.deleteOldSegments(new Function<LogSegment, Boolean>() {
//            @Override
//            public Boolean apply(LogSegment input) {
//                return true;
//            }
//        });
//        log.close();
//
//        log = new Log(logDir, config, 0L, time.scheduler, time);
//        assertEquals(log.numberOfSegments(), 1, "The deleted segments should be gone.");
//    }
//
//    @Test
//    public void testAppendMessageWithNullPayload() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
//        final Log log = new Log(logDir, logConfigBuilder.build(), 0L, time.scheduler, time);
//        final byte[] buffer = null;
//        log.append(new ByteBufferMessageSet(Lists.newArrayList(new Message(buffer))));
//        final MessageSet messageSet = log.read(0, 4096, Optional.<Long>absent()).getMessageSet();
//        assertEquals(Iterables.get(messageSet, 0).getOffset(), 0);
//        assertTrue(Iterables.get(messageSet, 0).getMessage().isNull(), "Message payload should be null.");
//    }
//
//    @Test
//    public void testCorruptLog() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        // append some messages to create some segments
//        final LogConfig config = logConfigBuilder.indexInterval(1).maxMessageSize(64 * 1024).segmentSize(1000).build();
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final long recoveryPoint = 50L;
//        for (int iteration = 0; iteration < 50; iteration++) {
//            // create a log and write some messages to it
//            logDir.mkdirs();
//            Log log = new Log(logDir, config, 0L, time.scheduler, time);
//            final int numMessages = 50 + TestUtils.RANDOM.nextInt(50);
//            for (int i = 0; i < numMessages; i++) {
//                log.append(set);
//            }
//
//            final List<MessageAndOffset> expectedMessages = Lists.newArrayList();
//            for (final LogSegment segment : log.logSegments()) {
//                expectedMessages.addAll(Lists.newArrayList(segment.getLog().iterator()));
//            }
//
//            log.close();
//
//            // corrupt index and log by appending random bytes
//            TestUtils.appendNonsenseToFile(log.activeSegment().getIndex().getFile(), TestUtils.RANDOM.nextInt(1024) + 1);
//            TestUtils.appendNonsenseToFile(log.activeSegment().getLog().getFile(), TestUtils.RANDOM.nextInt(1024) + 1);
//
//            // attempt recovery
//            log = new Log(logDir, config, recoveryPoint, time.scheduler, time);
//            assertEquals(log.logEndOffset(), numMessages);
//
//            final List<MessageAndOffset> messages = Lists.newArrayList();
//            for (final LogSegment segment : log.logSegments()) {
//                messages.addAll(Lists.newArrayList(segment.getLog().iterator()));
//            }
//            assertEquals(messages, expectedMessages, "Messages in the log after recovery should be the same.");
//            Utils.rm(logDir);
//        }
//    }
//
//    @Test
//    public void testCleanShutdownFile() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        // append some messages to create some segments
//        final LogConfig config = logConfigBuilder.indexInterval(1).maxMessageSize(64 * 1024).segmentSize(1000).build();
//        final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
//        final File parentLogDir = logDir.getParentFile();
//        assertTrue(parentLogDir.isDirectory(), String.format("Data directory %s must exist", parentLogDir));
//        final File cleanShutdownFile = new File(parentLogDir, Log.CLEAN_SHUTDOWN_FILE);
//        cleanShutdownFile.createNewFile();
//        assertTrue(cleanShutdownFile.exists(), ".kafka_cleanshutdown must exist");
//        long recoveryPoint = 0L;
//        // create a log and write some messages to it
//        Log log = new Log(logDir, config, recoveryPoint, time.scheduler, time);
//        for (int i = 0; i < 100; i++) {
//            log.append(set);
//        }
//        log.close();
//
//        // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
//        // clean shutdown file exists.
//        recoveryPoint = log.logEndOffset();
//        log = new Log(logDir, config, 0L, time.scheduler, time);
//        assertEquals(log.logEndOffset(), recoveryPoint);
//        cleanShutdownFile.delete();
//    }
//
//    @Test
//    public void testParseTopicPartitionName() throws Exception {
//        final String topic = "test_topic";
//        final String partition = "143";
//        final File dir = new File(logDir + topicPartitionName(topic, partition));
//        final TopicAndPartition topicAndPartition = Log.parseTopicPartitionName(dir);
//        assertEquals(topicAndPartition.getTopic(), topic);
//        assertEquals(topicAndPartition.getPartition(), Integer.parseInt(partition));
//    }
//
//    @Test
//    public void testParseTopicPartitionNameForEmptyName() throws Exception {
//        try {
//            final File dir = new File("");
//            final TopicAndPartition topicAndPartition = Log.parseTopicPartitionName(dir);
//            assertTrue(false, "KafkaException should have been thrown for dir: " + dir.getCanonicalPath());
//        } catch (Exception e) {
//            // its GOOD!
//        }
//    }
//
//    @Test
//    public void testParseTopicPartitionNameForNull() throws Exception {
//        try {
//            final File dir = null;
//            final TopicAndPartition topicAndPartition = Log.parseTopicPartitionName(dir);
//            assertTrue(false, "KafkaException should have been thrown for dir: " + dir);
//        } catch (Exception e) {
//            // its GOOD!
//        }
//    }
//
//    @Test
//    public void testParseTopicPartitionNameForMissingSeparator() throws Exception {
//        final String topic = "test_topic";
//        final String partition = "1999";
//        final File dir = new File(logDir + File.separator + topic + partition);
//        try {
//            final TopicAndPartition topicAndPartition = Log.parseTopicPartitionName(dir);
//            assertTrue(false, "KafkaException should have been thrown for dir: " + dir.getCanonicalPath());
//        } catch (Exception e) {
//            // its GOOD!
//        }
//    }
//
//    @Test
//    public void testParseTopicPartitionNameForMissingTopic() throws Exception {
//        final String topic = "";
//        final String partition = "1999";
//        final File dir = new File(logDir + topicPartitionName(topic, partition));
//        try {
//            final TopicAndPartition topicAndPartition = Log.parseTopicPartitionName(dir);
//            assertTrue(false, "KafkaException should have been thrown for dir: " + dir.getCanonicalPath());
//        } catch (Exception e) {
//            // its GOOD!
//        }
//    }
//
//    @Test
//    public void testParseTopicPartitionNameForMissingPartition() throws Exception {
//        final String topic = "test_topic";
//        final String partition = "";
//        final File dir = new File(logDir + topicPartitionName(topic, partition));
//        try {
//            final TopicAndPartition topicAndPartition = Log.parseTopicPartitionName(dir);
//            assertTrue(false, "KafkaException should have been thrown for dir: " + dir.getCanonicalPath());
//        } catch (Exception e) {
//            // its GOOD!
//        }
//    }
//
//    private String topicPartitionName(final String topic, final String partition) {
//        return File.separator + topic + "-" + partition;
//    }
}
