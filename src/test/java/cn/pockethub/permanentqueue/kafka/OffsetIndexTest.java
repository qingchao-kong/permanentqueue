package cn.pockethub.permanentqueue.kafka;

public class OffsetIndexTest {
//    private OffsetIndex idx;
//
//    @BeforeEach
//    public void setUp() throws Exception {
//        idx = new OffsetIndex(nonExistantTempFile(), 45L, 30 * 8);
//    }
//
//    @AfterEach
//    public void tearDown() throws Exception {
//        if (idx != null) {
//            idx.getFile().delete();
//        }
//    }
//
//    @Test
//    public void randomLookupTest() throws Exception {
//        assertEquals(idx.lookup(92L), new OffsetPosition(idx.getBaseOffset(), 0),
//                "Not present value should return physical offset 0.");
//
//        // append some random values
//        final int base = ((int) idx.getBaseOffset()) + 1;
//        final int size = idx.getMaxEntries();
//
//        final List<Pair<Long, Integer>> vals = Lists.newArrayList();
//        final List<Integer> integers1 = monotonicSeq(base, size);
//        final List<Integer> integers2 = monotonicSeq(0, size);
//
//        for (int i = 0; i < integers1.size(); i++) {
//            vals.add(ImmutablePair.of((long) integers1.get(i), integers2.get(i)));
//        }
//
//        for (final Pair<Long, Integer> pair : vals) {
//            idx.append(pair.getLeft(), pair.getRight());
//        }
//
//        // should be able to find all those values
//        for (final Pair<Long, Integer> pair : vals) {
//            final Long logical = pair.getLeft();
//            final Integer physical = pair.getRight();
//            assertEquals(idx.lookup(logical), new OffsetPosition(logical, physical),
//                    "Should be able to find values that are present.");
//        }
//
//        // for non-present values we should find the offset of the largest value less than or equal to this
//        final SortedMap<Long, Pair<Long, Integer>> valMap = Maps.newTreeMap();
//        for (final Pair<Long, Integer> pair : vals) {
//            valMap.put(pair.getLeft(), pair);
//        }
//
//        final List<Integer> offsets = Lists.newArrayList();
//        for (long i = idx.getBaseOffset(); i < Iterables.getLast(vals).getLeft(); i++) {
//            offsets.add((int) i);
//        }
//
//        Collections.shuffle(offsets);
//        for (final int offset : offsets.subList(0, 30)) {
//            OffsetPosition rightAnswer;
//
//            if (offset < valMap.firstKey()) {
//                rightAnswer = new OffsetPosition(idx.getBaseOffset(), 0);
//            } else {
//                // This was: new OffsetPosition(valMap.to(offset).last._1, valMap.to(offset).last._2._2);
//                rightAnswer = new OffsetPosition(valMap.headMap((long) offset + 1).lastKey(),
//                        Iterables.getLast(valMap.headMap((long) offset + 1).values()).getRight());
//            }
//
//            assertEquals(idx.lookup(offset), rightAnswer, "The index should give the same answer as the sorted map");
//        }
//    }
//
//    @Test
//    public void lookupExtremeCases() throws Exception {
//        assertEquals(idx.lookup(idx.getBaseOffset()), new OffsetPosition(idx.getBaseOffset(), 0), "Lookup on empty file");
//        for (int i = 0; i < idx.getMaxEntries(); i++) {
//            idx.append(idx.getBaseOffset() + i + 1, i);
//        }
//        // check first and last entry
//        assertEquals(idx.lookup(idx.getBaseOffset()), new OffsetPosition(idx.getBaseOffset(), 0));
//        assertEquals(idx.lookup(idx.getBaseOffset() + idx.getMaxEntries()),
//                new OffsetPosition(idx.getBaseOffset() + idx.getMaxEntries(), idx.getMaxEntries() - 1));
//    }
//
//    @Test
//    public void appendTooMany() throws Exception {
//        for (int i = 0; i < idx.getMaxEntries(); i++) {
//            final long offset = idx.getBaseOffset() + i + 1;
//            idx.append(offset, i);
//        }
//        assertWriteFails("Append should fail on a full index", idx, idx.getMaxEntries() + 1, IllegalArgumentException.class);
//    }
//
//    @Test
//    public void appendOutOfOrder() throws Exception {
//        idx.append(51, 0);
//        assertThrows(InvalidOffsetException.class, () -> idx.append(50, 1));
//    }
//
//    @Test
//    public void testReopen() throws Exception {
//        final OffsetPosition first = new OffsetPosition(51, 0);
//        final OffsetPosition sec = new OffsetPosition(52, 1);
//        idx.append(first.getOffset(), first.getPosition());
//        idx.append(sec.getOffset(), sec.getPosition());
//        idx.close();
//        final OffsetIndex idxRo = new OffsetIndex(idx.getFile(), idx.getBaseOffset());
//        assertEquals(idxRo.lookup(first.getOffset()), first);
//        assertEquals(idxRo.lookup(sec.getOffset()), sec);
//        assertEquals(idxRo.getLastOffset(), sec.getOffset());
//        assertEquals(idxRo.entries(), 2);
//        assertWriteFails("Append should fail on read-only index", idxRo, 53, IllegalArgumentException.class);
//    }
//
//    @Test
//    public void truncate() throws Exception {
//        final OffsetIndex idx = new OffsetIndex(nonExistantTempFile(), 0L, 10 * 8);
//        idx.truncate();
//        for (int i = 1; i < 10; i++) {
//            idx.append(i, i);
//        }
//
//        // now check the last offset after various truncate points and validate that we can still append to the index.
//        idx.truncateTo(12);
//        assertEquals(idx.lookup(10), new OffsetPosition(9, 9), "Index should be unchanged by truncate past the end");
//        assertEquals(idx.getLastOffset(), 9, "9 should be the last entry in the index");
//
//        idx.append(10, 10);
//        idx.truncateTo(10);
//        assertEquals(idx.lookup(10), new OffsetPosition(9, 9), "Index should be unchanged by truncate at the end");
//        assertEquals(idx.getLastOffset(), 9, "9 should be the last entry in the index");
//        idx.append(10, 10);
//
//        idx.truncateTo(9);
//        assertEquals(idx.lookup(10), new OffsetPosition(8, 8), "Index should truncate off last entry");
//        assertEquals(idx.getLastOffset(), 8, "8 should be the last entry in the index");
//        idx.append(9, 9);
//
//        idx.truncateTo(5);
//        assertEquals(idx.lookup(10), new OffsetPosition(4, 4), "4 should be the last entry in the index");
//        assertEquals(idx.getLastOffset(), 4, "4 should be the last entry in the index");
//        idx.append(5, 5);
//
//        idx.truncate();
//        assertEquals(idx.entries(), 0, "Full truncation should leave no entries");
//        idx.append(0, 0);
//    }
//
//    public <T> void assertWriteFails(final String message, final OffsetIndex idx, final int offset, T klass) {
//        try {
//            idx.append(offset, 1);
//            assertTrue(false, message);
//        } catch (Throwable e) {
//            assertEquals(e.getClass(), klass, "Got an unexpected exception.");
//        }
//    }
//
//    private List<Integer> monotonicSeq(final int base, final int len) {
//        final Random rand = new Random(1L);
//        final List<Integer> vals = Lists.newArrayList();
//        int last = base;
//        for (int i = 0; i < len; i++) {
//            last += rand.nextInt(15) + 1;
//            vals.add(last);
//        }
//        return vals;
//    }
//
//    private File nonExistantTempFile() throws IOException {
//        final File file = TestUtils.tempFile();
//        file.delete();
//        return file;
//    }
}
