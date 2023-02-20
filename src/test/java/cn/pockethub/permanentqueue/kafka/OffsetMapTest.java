package cn.pockethub.permanentqueue.kafka;

public class OffsetMapTest {
//    @Test
//    public void testBasicValidation() throws Exception, OffsetMapException {
//        validateMap(10);
//        validateMap(100);
//        validateMap(1000);
//        validateMap(5000);
//    }
//
//    @Test
//    public void testClear() throws Exception, OffsetMapException {
//        final OffsetMap map = new SkimpyOffsetMap(4000);
//        for (int i = 0; i < 10; i++) {
//            map.put(key(i), i);
//        }
//        for (int i = 0; i < 10; i++) {
//            assertEquals((long) i, map.get(key(i)));
//        }
//        map.clear();
//        for (int i = 0; i < 10; i++) {
//            assertEquals(map.get(key(i)), -1L);
//        }
//    }
//
//    private ByteBuffer key(int key) {
//        return ByteBuffer.wrap(String.valueOf(key).getBytes());
//    }
//
//    public SkimpyOffsetMap validateMap(final int items) throws OffsetMapException, NoSuchAlgorithmException {
//        return validateMap(items, 0.5);
//    }
//
//    public SkimpyOffsetMap validateMap(final int items, final double loadFactor) throws NoSuchAlgorithmException, OffsetMapException {
//        final SkimpyOffsetMap map = new SkimpyOffsetMap((int) ((items / loadFactor) * 24));
//        for (int i = 0; i < items; i++) {
//            map.put(key(i), i);
//        }
//        int misses = 0;
//        for (int i = 0; i < items; i++) {
//            assertEquals((long) i, map.get(key(i)));
//        }
//        return map;
//    }
//
//    public static void main(String[] args) throws OffsetMapException, NoSuchAlgorithmException {
//        if (args.length != 2) {
//            System.err.println("USAGE: java OffsetMapTest size load");
//            System.exit(1);
//        }
//        final OffsetMapTest test = new OffsetMapTest();
//        final int size = Integer.parseInt(args[0]);
//        final double load = Double.parseDouble(args[1]);
//        final long start = System.nanoTime();
//        final SkimpyOffsetMap map = test.validateMap(size, load);
//        final double ellapsedMs = (System.nanoTime() - start) / 1000.0 / 1000.0;
//        System.out.println(map.getSize() + " entries in map of size " + map.getSlots() + " in " + ellapsedMs + " ms");
//        System.out.println(String.format("Collision rate: %.1f%%", 100 * map.getCollisionRate()));
//    }
}
