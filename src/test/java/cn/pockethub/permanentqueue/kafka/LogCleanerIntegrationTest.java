package cn.pockethub.permanentqueue.kafka;

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
public class LogCleanerIntegrationTest {
//    private File logDir;
//    private LogConfigBuilder logConfigBuilder;
//
//    private MockTime time = new MockTime();
//    private int segmentSize = 100;
//    private int deleteDelay = 1000;
//    private String logName = "log";
//    private int counter = 0;
//    private List<TopicAndPartition> topics = Lists.newArrayList(new TopicAndPartition("log", 0),
//            new TopicAndPartition("log", 1), new TopicAndPartition("log", 2));
//
//    private LogCleaner cleaner;
//
//    @BeforeEach
//    public void setUp() throws Exception {
//        logDir = TestUtils.tempDir();
//        logConfigBuilder = new LogConfigBuilder();
//    }
//
//    @AfterEach
//    public void tearDown() throws Exception {
//        if (cleaner != null) {
//            cleaner.shutdown();
//        }
//        Utils.rm(logDir);
//    }
//
//    @Test
//    public void cleanerTest() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        cleaner = makeCleaner(3, 0, 1, "compact", new HashMap<String, String>());
//        final Log log = cleaner.getLogs().get(topics.get(0));
//
//        final Map<Integer, Integer> appends = writeDups(100, 3, log);
//        final long startSize = log.size();
//        cleaner.startup();
//
//        final long lastCleaned = log.activeSegment().getBaseOffset();
//        // wait until we clean up to base_offset of active segment - minDirtyMessages
//        cleaner.awaitCleaned("log", 0, lastCleaned, 30000L);
//
//        final Map<Integer, Integer> read = readFromLog(log);
//        assertEquals(read, appends, "Contents of the map shouldn't change.");
//        assertTrue(startSize > log.size());
//
//        // write some more stuff and validate again
//        final HashMap<Integer, Integer> appends2 = Maps.newHashMap(appends);
//        appends2.putAll(writeDups(100, 3, log));
//        final long lastCleaned2 = log.activeSegment().getBaseOffset();
//        cleaner.awaitCleaned("log", 0, lastCleaned2, 30000L);
//        final Map<Integer, Integer> read2 = readFromLog(log);
//        assertEquals(read2, appends2, "Contents of the map shouldn't change.");
//
//        // simulate deleting a partition, by removing it from logs
//        // force a checkpoint
//        // and make sure its gone from checkpoint file
//        cleaner.getLogs().remove(topics.get(0));
//
//        cleaner.updateCheckpoints(logDir);
//        final Map<TopicAndPartition, Long> checkpoints = new OffsetCheckpoint(new File(logDir, LogCleanerManager.OFFSET_CHECKPOINT_FILE)).read();
//
//        // we expect partition 0 to be gone
//        assertFalse(checkpoints.containsKey(topics.get(0)));
//    }
//
//    private Map<Integer, Integer> readFromLog(final Log log) throws UnsupportedEncodingException {
//        final Map<Integer, Integer> messages = Maps.newHashMap();
//
//        for (LogSegment segment : log.logSegments()) {
//            for (MessageAndOffset messageAndOffset : segment.getLog()) {
//                final Integer key = Integer.valueOf(Utils.readString(messageAndOffset.getMessage().key()));
//                final Integer value = Integer.valueOf(Utils.readString(messageAndOffset.getMessage().payload()));
//
//                messages.put(key, value);
//            }
//        }
//
//        return messages;
//    }
//
//    private Map<Integer, Integer> writeDups(final int numKeys, final int numDups, final Log log) throws IOException, SamsaStorageException, MessageSetSizeTooLargeException, MessageSizeTooLargeException, InvalidMessageSizeException {
//        final Map<Integer, Integer> map = Maps.newHashMap();
//
//        for (int dup = 0; dup < numDups; dup++) {
//            for (int key = 0; key < numKeys; key++) {
//                int count = counter;
//                log.append(TestUtils.singleMessageSet(String.valueOf(counter).getBytes(),
//                                CompressionCodec.NONE, String.valueOf(key).getBytes()),
//                        true);
//                counter += 1;
//                map.put(key, count);
//            }
//        }
//
//        return map;
//    }
//
//    /* create a cleaner instance and logs with the given parameters */
//    private LogCleaner makeCleaner(final int parts,
//                                   final int minDirtyMessages,
//                                   final int numThreads,
//                                   final String defaultPolicy,
//                                   final Map<String, String> policyOverrides) throws IOException, NoSuchAlgorithmException {
//        // create partitions and add them to the pool
//        final ConcurrentMap<TopicAndPartition, Log> logs = new ConcurrentHashMap<>();
//
//        for (int i = 0; i < parts; i++) {
//            final File dir = new File(logDir, "log-" + i);
//            dir.mkdirs();
//            final Log log = new Log(dir,
//                    logConfigBuilder.segmentSize(segmentSize)
//                            .maxIndexSize(100 * 1024)
//                            .fileDeleteDelayMs(deleteDelay)
//                            .compact(true).build(),
//                    0L,
//                    time.scheduler,
//                    time);
//            logs.put(new TopicAndPartition("log", i), log);
//        }
//
//        return new LogCleaner(new CleanerConfigBuilder().numThreads(numThreads).build(),
//                Lists.newArrayList(logDir),
//                logs,
//                time);
//    }
}
