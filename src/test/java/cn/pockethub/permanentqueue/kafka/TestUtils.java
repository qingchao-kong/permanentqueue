/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.pockethub.permanentqueue.kafka;

import cn.pockethub.permanentqueue.kafka.common.requests.ByteBufferChannel;
import cn.pockethub.permanentqueue.kafka.log.CleanerConfig;
import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogManager;
import cn.pockethub.permanentqueue.kafka.metadata.MockConfigRepository;
import cn.pockethub.permanentqueue.kafka.server.*;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.server.metrics.KafkaYammerMetrics;
import cn.pockethub.permanentqueue.kafka.test.NoRetryException;
import cn.pockethub.permanentqueue.kafka.test.TestCondition;
import cn.pockethub.permanentqueue.kafka.test.TestSslUtils;
import cn.pockethub.permanentqueue.kafka.test.ValuelessCallable;
import cn.pockethub.permanentqueue.kafka.utils.JaasTestUtils;
import cn.pockethub.permanentqueue.kafka.utils.Logging;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Helper functions for writing unit tests
 */
public class TestUtils extends Logging {

    /*-----------from scala --------------*/
    public static final File IO_TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String DIGITS = "0123456789";
    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    /* A consistent random number generator to make tests repeatable */
    public static final Random SEEDED_RANDOM = new Random(192348092834L);
    public static final Random RANDOM = new Random();
    public static final long DEFAULT_POLL_INTERVAL_MS = 100;
    public static final long DEFAULT_MAX_WAIT_MS = 15000;

    public static Cluster singletonCluster() {
        return clusterWith(1);
    }

    public static Cluster singletonCluster(final String topic, final int partitions) {
        return clusterWith(1, topic, partitions);
    }

    public static Cluster clusterWith(int nodes) {
        return clusterWith(nodes, new HashMap<>());
    }

    public static Cluster clusterWith(final int nodes, final Map<String, Integer> topicPartitionCounts) {
        final Node[] ns = new Node[nodes];
        for (int i = 0; i < nodes; i++)
            ns[i] = new Node(i, "localhost", 1969);
        final List<PartitionInfo> parts = new ArrayList<>();
        for (final Map.Entry<String, Integer> topicPartition : topicPartitionCounts.entrySet()) {
            final String topic = topicPartition.getKey();
            final int partitions = topicPartition.getValue();
            for (int i = 0; i < partitions; i++)
                parts.add(new PartitionInfo(topic, i, ns[i % ns.length], ns, ns));
        }
        return new Cluster("kafka-cluster", asList(ns), parts, Collections.emptySet(), Collections.emptySet());
    }

    public static Cluster clusterWith(final int nodes, final String topic, final int partitions) {
        return clusterWith(nodes, Collections.singletonMap(topic, partitions));
    }

    /**
     * Generate an array of random bytes
     *
     * @param size The size of the array
     */
    public static byte[] randomBytes(final int size) {
        final byte[] bytes = new byte[size];
        SEEDED_RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generate a random string of letters and digits of the given length
     *
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

    /**
     * Create an empty file in the default temporary-file directory, using the given prefix and suffix
     * to generate its name.
     *
     * @throws IOException
     */
    public static File tempFile(final String prefix, final String suffix) throws IOException {
        final File file = Files.createTempFile(prefix, suffix).toFile();
        file.deleteOnExit();

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                logger.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        return tempFile("kafka", ".tmp");
    }

    /**
     * Create a file with the given contents in the default temporary-file directory,
     * using `kafka` as the prefix and `tmp` as the suffix to generate its name.
     */
    public static File tempFile(final String contents) throws IOException {
        final File file = tempFile();
        Files.write(file.toPath(), contents.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with the given prefix.
     *
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(final String prefix) {
        return tempDirectory(null, prefix);
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with a
     * prefix of "kafka-"
     *
     * @return the temporary directory just created.
     */
    public static File tempDirectory() {
        return tempDirectory(null);
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     * @param parent The parent folder path name, if null using the default temporary-file directory
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(final Path parent, String prefix) {
        final File file;
        prefix = prefix == null ? "kafka-" : prefix;
        try {
            file = parent == null ?
                    Files.createTempDirectory(prefix).toFile() : Files.createTempDirectory(parent, prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                logger.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    public static Properties producerConfig(final String bootstrapServers,
                                            final Class<?> keySerializer,
                                            final Class<?> valueSerializer,
                                            final Properties additional) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.putAll(additional);
        return properties;
    }

    public static Properties producerConfig(final String bootstrapServers, final Class<?> keySerializer, final Class<?> valueSerializer) {
        return producerConfig(bootstrapServers, keySerializer, valueSerializer, new Properties());
    }

    public static Properties consumerConfig(final String bootstrapServers,
                                            final String groupId,
                                            final Class<?> keyDeserializer,
                                            final Class<?> valueDeserializer,
                                            final Properties additional) {

        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        consumerConfig.putAll(additional);
        return consumerConfig;
    }

    public static Properties consumerConfig(final String bootstrapServers,
                                            final String groupId,
                                            final Class<?> keyDeserializer,
                                            final Class<?> valueDeserializer) {
        return consumerConfig(bootstrapServers,
                groupId,
                keyDeserializer,
                valueDeserializer,
                new Properties());
    }

    /**
     * returns consumer config with random UUID for the Group ID
     */
    public static Properties consumerConfig(final String bootstrapServers, final Class<?> keyDeserializer, final Class<?> valueDeserializer) {
        return consumerConfig(bootstrapServers,
                UUID.randomUUID().toString(),
                keyDeserializer,
                valueDeserializer,
                new Properties());
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final TestCondition testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, () -> conditionDetails);
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final TestCondition testCondition, final Supplier<String> conditionDetailsSupplier) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, conditionDetailsSupplier);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs, String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, () -> conditionDetails);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs, Supplier<String> conditionDetailsSupplier) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, DEFAULT_POLL_INTERVAL_MS, conditionDetailsSupplier);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} with a polling interval of {@code pollIntervalMs}
     * and throw assertion failure otherwise. This should be used instead of {@code Thread.sleep} whenever possible
     * as it allows a longer timeout to be used without unnecessarily increasing test time (as the condition is
     * checked frequently). The longer timeout is needed to avoid transient failures due to slow or overloaded
     * machines.
     */
    public static void waitForCondition(
            final TestCondition testCondition,
            final long maxWaitMs,
            final long pollIntervalMs,
            Supplier<String> conditionDetailsSupplier
    ) throws InterruptedException {
        retryOnExceptionWithTimeout(maxWaitMs, pollIntervalMs, () -> {
            String conditionDetailsSupplied = conditionDetailsSupplier != null ? conditionDetailsSupplier.get() : null;
            String conditionDetails = conditionDetailsSupplied != null ? conditionDetailsSupplied : "";
            assertTrue(testCondition.conditionMet(),
                    "Condition not met within timeout " + maxWaitMs + ". " + conditionDetails);
        });
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs the total time in milliseconds to wait for {@code runnable} to complete successfully.
     * @param runnable  the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final long timeoutMs,
                                                   final ValuelessCallable runnable) throws InterruptedException {
        retryOnExceptionWithTimeout(timeoutMs, DEFAULT_POLL_INTERVAL_MS, runnable);
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the default timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final ValuelessCallable runnable) throws InterruptedException {
        retryOnExceptionWithTimeout(DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, runnable);
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs      the total time in milliseconds to wait for {@code runnable} to complete successfully.
     * @param pollIntervalMs the interval in milliseconds to wait between invoking {@code runnable}.
     * @param runnable       the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final long timeoutMs,
                                                   final long pollIntervalMs,
                                                   final ValuelessCallable runnable) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + timeoutMs;

        while (true) {
            try {
                runnable.call();
                return;
            } catch (final NoRetryException e) {
                throw e;
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(String.format("Assertion failed with an exception after %s ms", timeoutMs), e);
                }
            }
            Thread.sleep(Math.min(pollIntervalMs, timeoutMs));
        }
    }

    /**
     * Checks if a cluster id is valid.
     *
     * @param clusterId
     */
    public static void isValidClusterId(String clusterId) {
        assertNotNull(clusterId);

        // Base 64 encoded value is 22 characters
        assertEquals(clusterId.length(), 22);

        Pattern clusterIdPattern = Pattern.compile("[a-zA-Z0-9_\\-]+");
        Matcher matcher = clusterIdPattern.matcher(clusterId);
        assertTrue(matcher.matches());

        // Convert into normal variant and add padding at the end.
        String originalClusterId = String.format("%s==", clusterId.replace("_", "/").replace("-", "+"));
        byte[] decodedUuid = Base64.getDecoder().decode(originalClusterId);

        // We expect 16 bytes, same as the input UUID.
        assertEquals(decodedUuid.length, 16);

        //Check if it can be converted back to a UUID.
        try {
            ByteBuffer uuidBuffer = ByteBuffer.wrap(decodedUuid);
            new UUID(uuidBuffer.getLong(), uuidBuffer.getLong()).toString();
        } catch (Exception e) {
            fail(clusterId + " cannot be converted back to UUID.");
        }
    }

    /**
     * Checks the two iterables for equality by first converting both to a list.
     */
    public static <T> void checkEquals(Iterable<T> it1, Iterable<T> it2) {
        assertEquals(toList(it1), toList(it2));
    }

    public static <T> void checkEquals(Iterator<T> it1, Iterator<T> it2) {
        assertEquals(Utils.toList(it1), Utils.toList(it2));
    }

    public static <T> void checkEquals(Set<T> c1, Set<T> c2, String firstDesc, String secondDesc) {
        if (!c1.equals(c2)) {
            Set<T> missing1 = new HashSet<>(c2);
            missing1.removeAll(c1);
            Set<T> missing2 = new HashSet<>(c1);
            missing2.removeAll(c2);
            fail(String.format("Sets not equal, missing %s=%s, missing %s=%s", firstDesc, missing1, secondDesc, missing2));
        }
    }

    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        List<T> list = new ArrayList<>();
        for (T item : iterable)
            list.add(item);
        return list;
    }

    public static <T> Set<T> toSet(Collection<T> collection) {
        return new HashSet<>(collection);
    }

    public static ByteBuffer toBuffer(Send send) {
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        try {
            assertEquals(send.size(), send.writeTo(channel));
            channel.close();
            return channel.buffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer toBuffer(UnalignedRecords records) {
        return toBuffer(records.toSend());
    }

    public static Set<TopicPartition> generateRandomTopicPartitions(int numTopic, int numPartitionPerTopic) {
        Set<TopicPartition> tps = new HashSet<>();
        for (int i = 0; i < numTopic; i++) {
            String topic = randomString(32);
            for (int j = 0; j < numPartitionPerTopic; j++) {
                tps.add(new TopicPartition(topic, j));
            }
        }
        return tps;
    }

    /**
     * Assert that a future raises an expected exception cause type. Return the exception cause
     * if the assertion succeeds; otherwise raise AssertionError.
     *
     * @param future              The future to await
     * @param exceptionCauseClass Class of the expected exception cause
     * @param <T>                 Exception cause type parameter
     * @return The caught exception cause
     */
    public static <T extends Throwable> T assertFutureThrows(Future<?> future, Class<T> exceptionCauseClass) {
        ExecutionException exception = assertThrows(ExecutionException.class, future::get);
        assertTrue(exceptionCauseClass.isInstance(exception.getCause()),
                "Unexpected exception cause " + exception.getCause());
        return exceptionCauseClass.cast(exception.getCause());
    }

    public static <T extends Throwable> void assertFutureThrows(
            Future<?> future,
            Class<T> expectedCauseClassApiException,
            String expectedMessage
    ) {
        T receivedException = assertFutureThrows(future, expectedCauseClassApiException);
        assertEquals(expectedMessage, receivedException.getMessage());
    }

    public static void assertFutureError(Future<?> future, Class<? extends Throwable> exceptionClass)
            throws InterruptedException {
        try {
            future.get();
            fail("Expected a " + exceptionClass.getSimpleName() + " exception, but got success.");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertEquals(exceptionClass, cause.getClass(),
                    "Expected a " + exceptionClass.getSimpleName() + " exception, but got " +
                            cause.getClass().getSimpleName());
        }
    }

    public static ApiKeys apiKeyFrom(NetworkReceive networkReceive) {
        return RequestHeader.parse(networkReceive.payload().duplicate()).apiKey();
    }

    public static <T> void assertOptional(Optional<T> optional, Consumer<T> assertion) {
        if (optional.isPresent()) {
            assertion.accept(optional.get());
        } else {
            fail("Missing value from Optional");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fieldValue(Object o, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    /**
     * Returns true if both iterators have same elements in the same order.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param <T>       type of element in the iterators.
     */
    public static <T> boolean sameElementsWithOrder(Iterator<T> iterator1,
                                                    Iterator<T> iterator2) {
        while (iterator1.hasNext()) {
            if (!iterator2.hasNext()) {
                return false;
            }

            if (!Objects.equals(iterator1.next(), iterator2.next())) {
                return false;
            }
        }

        return !iterator2.hasNext();
    }

    /**
     * Returns true if both the iterators have same set of elements irrespective of order and duplicates.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param <T>       type of element in the iterators.
     */
    public static <T> boolean sameElementsWithoutOrder(Iterator<T> iterator1,
                                                       Iterator<T> iterator2) {
        // Check both the iterators have the same set of elements irrespective of order and duplicates.
        Set<T> allSegmentsSet = new HashSet<>();
        iterator1.forEachRemaining(allSegmentsSet::add);
        Set<T> expectedSegmentsSet = new HashSet<>();
        iterator2.forEachRemaining(expectedSegmentsSet::add);

        return allSegmentsSet.equals(expectedSegmentsSet);
    }

    /*-----------from java --------------*/
    public static final Random random = RANDOM;


    /* 0 gives a random port; you can then retrieve the assigned port from the Socket object. */
    public static final int RandomPort = 0;

    /* Incorrect broker port which can used by kafka clients in tests. This port should not be used
     by any other service and hence we use a reserved port. */
    public static final int IncorrectBrokerPort = 225;

    /**
     * Port to use for unit tests that mock/don't require a real ZK server.
     */
    public static final int MockZkPort = 1;
    /**
     * ZooKeeper connection string to use for unit tests that mock/don't require a real ZK server.
     */
    public static final String MockZkConnect = "127.0.0.1:" + MockZkPort;
    // CN in SSL certificates - this is used for endpoint validation when enabled
    public static final String SslCertificateCn = "localhost";

    private static final String transactionStatusKey = "transactionStatus";
    private static final byte[] committedValue = "committed".getBytes(StandardCharsets.UTF_8);
    private static final byte[] abortedValue = "aborted".getBytes(StandardCharsets.UTF_8);

    public enum LogDirFailureType {
        Roll,
        Checkpoint,
        ;
    }

    /**
     * Create a temporary directory
     */
    public static File tempDir() {
        return tempDirectory();
    }

    public static String tempTopic() {
        return "testTopic" + random.nextInt(1000000);
    }

    /**
     * Create a temporary relative directory
     */
    public static File tempRelativeDir(String parent) {
        File parentFile = new File(parent);
        parentFile.mkdirs();

        return tempDirectory(parentFile.toPath(), null);
    }

    /**
     * Create a random log directory in the format <string>-<int> used for Kafka partition logs.
     * It is the responsibility of the caller to set up a shutdown hook for deletion of the directory.
     */
    public static File randomPartitionLogDir(File parentDir) {
        int attempts = 1000;
        File f = null;
        for (int i = 0; i < attempts; i++) {
            File file = new File(parentDir, "kafka-" + random.nextInt(1000000));
            if (file.mkdir()) {
                f = file;
                break;
            }
        }
        if (Objects.isNull(f)) {
            throw new RuntimeException(String.format("Failed to create directory after %s attempts", attempts));
        }
        f.deleteOnExit();
        return f;
    }

    /**
     * Create a temporary file and return an open file channel for this file
     */
    public static FileChannel tempChannel() throws IOException {
        return FileChannel.open(tempFile().toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

//    /**
//     * Create a kafka server instance with appropriate test settings
//     * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
//     *
//     * @param config The configuration of the server
//     */
//    public static KafkaServer createServer(KafkaConfig config , Time time){
//        return createServer(config, time, Optional.empty());
//    }
//
//    public static KafkaServer createServer(KafkaConfig config,Optional<String> threadNamePrefix) {
//      return   createServer(config, Time.SYSTEM, threadNamePrefix);
//    }
//
//    public static KafkaServer createServer(KafkaConfig config,Time time,Optional<String> threadNamePrefix) {
//       return createServer(config, time, threadNamePrefix,  true);
//    }

//    public static KafkaServer createServer(KafkaConfig config,Time time, Optional<String> threadNamePrefix,Boolean startup) {
//        KafkaServer server = new KafkaServer(config, time, threadNamePrefix, enableForwarding = false);
//        if (startup) {
//            server.startup();
//        }
//        return server;
//    }
//
//    public static Integer boundPort(KafkaBroker broker, SecurityProtocol securityProtocol){
//        return broker.boundPort(ListenerName.forSecurityProtocol(securityProtocol));
//    }
//
//    public static Pair<Broker,Long> createBrokerAndEpoch(Integer id,
//                                                         String host,
//                                                         Integer port,
//                                                         SecurityProtocol securityProtocol,
//                                                         Long epoch){
//       return  Pair.of(new Broker(id, host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol), epoch);
//    }

    /**
     * Create a test config for the provided parameters.
     * <p>
     * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
     */
    public static List<Properties> createBrokerConfigs(Integer numConfigs,
                                                       String zkConnect,
                                                       Boolean enableControlledShutdown,
                                                       Boolean enableDeleteTopic,
                                                       Optional<SecurityProtocol> interBrokerSecurityProtocol,
                                                       Optional<File> trustStoreFile,
                                                       Optional<Properties> saslProperties,
                                                       Boolean enablePlaintext,
                                                       Boolean enableSsl,
                                                       Boolean enableSaslPlaintext,
                                                       Boolean enableSaslSsl,
                                                       Map<Integer, String> rackInfo,
                                                       Integer logDirCount,
                                                       Boolean enableToken,
                                                       Integer numPartitions,
                                                       Short defaultReplicationFactor,
                                                       Integer startingIdNumber) throws Exception {
        int endingIdNumber = startingIdNumber + numConfigs - 1;
        List<Properties> res = new ArrayList<>();
        for (int node = startingIdNumber; node <= endingIdNumber; node++) {
            Properties properties = createBrokerConfig(node,
                    zkConnect,
                    enableControlledShutdown,
                    enableDeleteTopic,
                    RandomPort,
                    interBrokerSecurityProtocol,
                    trustStoreFile,
                    saslProperties,
                    enablePlaintext,
                    enableSaslPlaintext,
                    RandomPort,
                    enableSsl,
                    RandomPort,
                    enableSaslSsl,
                    RandomPort,
                    Optional.ofNullable(rackInfo.get(node)),
                    logDirCount,
                    enableToken,
                    numPartitions,
                    defaultReplicationFactor);
            res.add(properties);
        }
        return res;
    }
//
//    def plaintextBootstrapServers[B <: KafkaBroker](
//    brokers: Seq[B]
//            ): String = {
//        bootstrapServers(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
//    }
//
//    def bootstrapServers[B <: KafkaBroker](
//    brokers: Seq[B],
//    listenerName: ListenerName
//  ): String = {
//        brokers.map { s =>
//            val listener = s.config.effectiveAdvertisedListeners.find(_.listenerName == listenerName).getOrElse(
//                    sys.error(s"Could not find listener with name ${listenerName.value}"))
//            formatAddress(listener.host, s.boundPort(listenerName))
//        }.mkString(",")
//    }
//
//    /**
//     * Shutdown `servers` and delete their log directories.
//     */
//    def shutdownServers[B <: KafkaBroker](brokers: Seq[B], deleteLogDirs: Boolean = true): Unit = {
//    import ExecutionContext.Implicits._
//        val future = Future.traverse(brokers) { s =>
//            Future {
//                s.shutdown()
//                if (deleteLogDirs) CoreUtils.delete(s.config.logDirs)
//            }
//        }
//        Await.result(future, FiniteDuration(5, TimeUnit.MINUTES))
//    }

    public static Properties createBrokerConfig(Integer nodeId,
                                                String zkConnect,
                                                Integer port) throws Exception {
        return createBrokerConfig(nodeId,
                zkConnect,
                true,
                true,
                port,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                false,
                RandomPort,
                false,
                RandomPort,
                false,
                RandomPort,
                Optional.empty(),
                1,
                false,
                1,
                (short) 1);
    }

    /**
     * Create a test config for the provided parameters.
     * <p>
     * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
     */
    public static Properties createBrokerConfig(Integer nodeId,
                                                String zkConnect,
                                                Boolean enableControlledShutdown,
                                                Boolean enableDeleteTopic,
                                                Integer port,
                                                Optional<SecurityProtocol> interBrokerSecurityProtocol,
                                                Optional<File> trustStoreFile,
                                                Optional<Properties> saslProperties,
                                                Boolean enablePlaintext,
                                                Boolean enableSaslPlaintext,
                                                Integer saslPlaintextPort,
                                                Boolean enableSsl,
                                                Integer sslPort,
                                                Boolean enableSaslSsl,
                                                Integer saslSslPort,
                                                Optional<String> rack,
                                                Integer logDirCount,
                                                Boolean enableToken,
                                                Integer numPartitions,
                                                Short defaultReplicationFactor) throws Exception {
        List<Pair<SecurityProtocol, Integer>> protocolAndPorts = new ArrayList<>();
        if (enablePlaintext || shouldEnable(SecurityProtocol.PLAINTEXT, interBrokerSecurityProtocol)) {
            protocolAndPorts.add(Pair.of(SecurityProtocol.PLAINTEXT, port));
        }
        if (enableSsl || shouldEnable(SecurityProtocol.SSL, interBrokerSecurityProtocol)) {
            protocolAndPorts.add(Pair.of(SecurityProtocol.SSL, sslPort));
        }
        if (enableSaslPlaintext || shouldEnable(SecurityProtocol.SASL_PLAINTEXT, interBrokerSecurityProtocol)) {
            protocolAndPorts.add(Pair.of(SecurityProtocol.SASL_PLAINTEXT, saslPlaintextPort));
        }
        if (enableSaslSsl || shouldEnable(SecurityProtocol.SASL_SSL, interBrokerSecurityProtocol)) {
            protocolAndPorts.add(Pair.of(SecurityProtocol.SASL_SSL, saslSslPort));
        }
        String listeners = protocolAndPorts.stream()
                .map(pair -> String.format("%s://localhost:%s", pair.getKey().name, pair.getValue()))
                .collect(Collectors.joining(","));

        Properties props = new Properties();
        if (zkConnect == null) {
            props.put(KafkaConfig.NodeIdProp, nodeId.toString());
            props.put(KafkaConfig.BrokerIdProp, nodeId.toString());
            props.put(KafkaConfig.AdvertisedListenersProp, listeners);
            props.put(KafkaConfig.ListenersProp, listeners);
            props.put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER");
            String str = protocolAndPorts.stream()
                    .map(pair -> String.format("%s:%s", pair.getKey(), pair.getKey()))
                    .collect(Collectors.joining(","));
            props.put(KafkaConfig.ListenerSecurityProtocolMapProp, str + ",CONTROLLER:PLAINTEXT");
        } else {
            if (nodeId >= 0) {
                props.put(KafkaConfig.BrokerIdProp, nodeId.toString());
            }
            props.put(KafkaConfig.ListenersProp, listeners);
        }
        if (logDirCount > 1) {
            List<String> logDirList = new ArrayList<>();
            // We would like to allow user to specify both relative path and absolute path as log directory for backward-compatibility reason
            // We can verify this by using a mixture of relative path and absolute path as log directories in the test
            for (int i = 1; i <= logDirCount; i++) {
                if (i % 2 == 0) {
                    logDirList.add(tempDir().getAbsolutePath());
                } else {
                    logDirList.add(tempRelativeDir("data").toString());
                }
            }
            String logDirs = String.join(",", logDirList);
            props.put(KafkaConfig.LogDirsProp, logDirs);
        } else {
            props.put(KafkaConfig.LogDirProp, tempDir().getAbsolutePath());
        }
        if (zkConnect == null) {
            props.put(KafkaConfig.ProcessRolesProp, "broker");
            // Note: this is just a placeholder value for controller.quorum.voters. JUnit
            // tests use random port assignment, so the controller ports are not known ahead of
            // time. Therefore, we ignore controller.quorum.voters and use
            // controllerQuorumVotersFuture instead.
            props.put(KafkaConfig.QuorumVotersProp, "1000@localhost:0");
        } else {
            props.put(KafkaConfig.ZkConnectProp, zkConnect);
            props.put(KafkaConfig.ZkConnectionTimeoutMsProp, "10000");
        }
        props.put(KafkaConfig.ReplicaSocketTimeoutMsProp, "1500");
        props.put(KafkaConfig.ControllerSocketTimeoutMsProp, "1500");
        props.put(KafkaConfig.ControlledShutdownEnableProp, enableControlledShutdown.toString());
        props.put(KafkaConfig.DeleteTopicEnableProp, enableDeleteTopic.toString());
        props.put(KafkaConfig.LogDeleteDelayMsProp, "1000");
        props.put(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "100");
        props.put(KafkaConfig.LogCleanerDedupeBufferSizeProp, "2097152");
        props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, Long.toString(Long.MAX_VALUE));
        props.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1");
        if (!props.containsKey(KafkaConfig.OffsetsTopicPartitionsProp)) {
            props.put(KafkaConfig.OffsetsTopicPartitionsProp, "5");
        }
        if (!props.containsKey(KafkaConfig.GroupInitialRebalanceDelayMsProp)) {
            props.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0");
        }
        rack.ifPresent(s -> props.put(KafkaConfig.RackProp, s));

        if (protocolAndPorts.stream().anyMatch(pair -> usesSslTransportLayer(pair.getKey()))) {
            props.putAll(sslConfigs(Mode.SERVER, false, trustStoreFile, String.format("server%s", nodeId)));
        }

        if (protocolAndPorts.stream().anyMatch(pair -> usesSaslAuthentication(pair.getKey()))) {
            props.putAll(JaasTestUtils.saslConfigs(saslProperties));
        }

        interBrokerSecurityProtocol.ifPresent(protocol -> {
            props.put(KafkaConfig.InterBrokerSecurityProtocolProp, protocol.name);
        });

        if (enableToken) {
            props.put(KafkaConfig.DelegationTokenSecretKeyProp, "secretkey");
        }

        props.put(KafkaConfig.NumPartitionsProp, numPartitions.toString());
        props.put(KafkaConfig.DefaultReplicationFactorProp, defaultReplicationFactor.toString());

        return props;
    }

    private static Boolean shouldEnable(SecurityProtocol protocol, Optional<SecurityProtocol> interBrokerSecurityProtocol) {
        if (interBrokerSecurityProtocol.isPresent()) {
            return interBrokerSecurityProtocol.get() == protocol;
        } else {
            return false;
        }
    }


//
//    @nowarn("cat=deprecation")
//    def setIbpAndMessageFormatVersions(config: Properties, version: MetadataVersion): Unit = {
//        config.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, version.version)
//        // for clarity, only set the log message format version if it's not ignored
//        if (!LogConfig.shouldIgnoreMessageFormatVersion(version))
//            config.setProperty(KafkaConfig.LogMessageFormatVersionProp, version.version)
//    }
//
//    def createAdminClient[B <: KafkaBroker](
//    brokers: Seq[B],
//    listenerName: ListenerName,
//    adminConfig: Properties = new Properties
//  ): Admin = {
//        val adminClientProperties = new Properties()
//        adminClientProperties.putAll(adminConfig)
//        if (!adminClientProperties.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)) {
//            adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(brokers, listenerName))
//        }
//        Admin.create(adminClientProperties)
//    }
//
//    def createTopicWithAdminRaw[B <: KafkaBroker](
//    admin: Admin,
//    topic: String,
//    numPartitions: Int = 1,
//    replicationFactor: Int = 1,
//    replicaAssignment: collection.Map[Int, Seq[Int]] = Map.empty,
//    topicConfig: Properties = new Properties,
//            ): Uuid = {
//        val configsMap = new util.HashMap[String, String]()
//        topicConfig.forEach((k, v) => configsMap.put(k.toString, v.toString))
//
//        val result = if (replicaAssignment.isEmpty) {
//            admin.createTopics(Collections.singletonList(new NewTopic(
//                    topic, numPartitions, replicationFactor.toShort).configs(configsMap)))
//        } else {
//            val assignment = new util.HashMap[Integer, util.List[Integer]]()
//            replicaAssignment.forKeyValue { case (k, v) =>
//                val replicas = new util.ArrayList[Integer]
//                v.foreach(r => replicas.add(r.asInstanceOf[Integer]))
//                assignment.put(k.asInstanceOf[Integer], replicas)
//            }
//            admin.createTopics(Collections.singletonList(new NewTopic(
//                    topic, assignment).configs(configsMap)))
//        }
//
//        result.topicId(topic).get()
//    }
//
//    def createTopicWithAdmin[B <: KafkaBroker](
//    admin: Admin,
//    topic: String,
//    brokers: Seq[B],
//    numPartitions: Int = 1,
//    replicationFactor: Int = 1,
//    replicaAssignment: collection.Map[Int, Seq[Int]] = Map.empty,
//    topicConfig: Properties = new Properties,
//            ): scala.collection.immutable.Map[Int, Int] = {
//        val effectiveNumPartitions = if (replicaAssignment.isEmpty) {
//            numPartitions
//        } else {
//            replicaAssignment.size
//        }
//
//        try {
//            createTopicWithAdminRaw(
//                    admin,
//                    topic,
//                    numPartitions,
//                    replicationFactor,
//                    replicaAssignment,
//                    topicConfig
//            )
//        } catch {
//            case e: ExecutionException => if (!(e.getCause != null &&
//                    e.getCause.isInstanceOf[TopicExistsException] &&
//                    topicHasSameNumPartitionsAndReplicationFactor(admin, topic,
//                            effectiveNumPartitions, replicationFactor))) {
//                throw e
//            }
//        }
//
//        // wait until we've propagated all partitions metadata to all brokers
//        val allPartitionsMetadata = waitForAllPartitionsMetadata(brokers, topic, effectiveNumPartitions)
//
//        (0 until effectiveNumPartitions).map { i =>
//            i -> allPartitionsMetadata.get(new TopicPartition(topic, i)).map(_.leader()).getOrElse(
//            throw new IllegalStateException(s"Cannot get the partition leader for topic: $topic, partition: $i in server metadata cache"))
//        }.toMap
//    }
//
//    def describeTopic(
//            admin: Admin,
//            topic: String
//    ): TopicDescription = {
//        val describedTopics = admin.describeTopics(
//                Collections.singleton(topic)
//        ).allTopicNames().get()
//        describedTopics.get(topic)
//    }
//
//    def topicHasSameNumPartitionsAndReplicationFactor(adminClient: Admin,
//                                                      topic: String,
//                                                      numPartitions: Int,
//                                                      replicationFactor: Int): Boolean = {
//        val description = describeTopic(adminClient, topic)
//        description != null &&
//                description.partitions().size() == numPartitions &&
//                description.partitions().iterator().next().replicas().size() == replicationFactor
//    }
//
//    def createOffsetsTopicWithAdmin[B <: KafkaBroker](
//    admin: Admin,
//    brokers: Seq[B]
//            ): Map[Int, Int] = {
//        val broker = brokers.head
//        createTopicWithAdmin(
//                admin = admin,
//                topic = Topic.GROUP_METADATA_TOPIC_NAME,
//                numPartitions = broker.config.getInt(KafkaConfig.OffsetsTopicPartitionsProp),
//                replicationFactor = broker.config.getShort(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
//                brokers = brokers,
//                topicConfig = broker.groupCoordinator.offsetsTopicConfigs,
//                )
//    }
//
//    def deleteTopicWithAdmin[B <: KafkaBroker](
//    admin: Admin,
//    topic: String,
//    brokers: Seq[B],
//            ): Unit = {
//        try {
//            admin.deleteTopics(Collections.singletonList(topic)).all().get()
//        } catch {
//            case e: ExecutionException if e.getCause != null &&
//                    e.getCause.isInstanceOf[UnknownTopicOrPartitionException] =>
//                // ignore
//        }
//        waitForAllPartitionsMetadata(brokers, topic, 0)
//    }
//
//    /**
//     * Create a topic in ZooKeeper.
//     * Wait until the leader is elected and the metadata is propagated to all brokers.
//     * Return the leader for each partition.
//     */
//    def createTopic(zkClient: KafkaZkClient,
//                    topic: String,
//                    numPartitions: Int = 1,
//                    replicationFactor: Int = 1,
//                    servers: Seq[KafkaBroker],
//                    topicConfig: Properties = new Properties): scala.collection.immutable.Map[Int, Int] = {
//        val adminZkClient = new AdminZkClient(zkClient)
//        // create topic
//        waitUntilTrue( () => {
//                var hasSessionExpirationException = false
//        try {
//            adminZkClient.createTopic(topic, numPartitions, replicationFactor, topicConfig)
//        } catch {
//            case _: SessionExpiredException => hasSessionExpirationException = true
//            case e: Throwable => throw e // let other exceptions propagate
//        }
//        !hasSessionExpirationException},
//        s"Can't create topic $topic")
//
//        // wait until we've propagated all partitions metadata to all servers
//        val allPartitionsMetadata = waitForAllPartitionsMetadata(servers, topic, numPartitions)
//
//        (0 until numPartitions).map { i =>
//            i -> allPartitionsMetadata.get(new TopicPartition(topic, i)).map(_.leader()).getOrElse(
//            throw new IllegalStateException(s"Cannot get the partition leader for topic: $topic, partition: $i in server metadata cache"))
//        }.toMap
//    }
//
//    /**
//     * Create a topic in ZooKeeper using a customized replica assignment.
//     * Wait until the leader is elected and the metadata is propagated to all brokers.
//     * Return the leader for each partition.
//     */
//    def createTopic(zkClient: KafkaZkClient,
//                    topic: String,
//                    partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
//                    servers: Seq[KafkaBroker]): scala.collection.immutable.Map[Int, Int] = {
//        createTopic(zkClient, topic, partitionReplicaAssignment, servers, new Properties())
//    }
//
//    /**
//     * Create a topic in ZooKeeper using a customized replica assignment.
//     * Wait until the leader is elected and the metadata is propagated to all brokers.
//     * Return the leader for each partition.
//     */
//    def createTopic(zkClient: KafkaZkClient,
//                    topic: String,
//                    partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
//                    servers: Seq[KafkaBroker],
//                    topicConfig: Properties): scala.collection.immutable.Map[Int, Int] = {
//        val adminZkClient = new AdminZkClient(zkClient)
//        // create topic
//        waitUntilTrue( () => {
//                var hasSessionExpirationException = false
//        try {
//            adminZkClient.createTopicWithAssignment(topic, topicConfig, partitionReplicaAssignment)
//        } catch {
//            case _: SessionExpiredException => hasSessionExpirationException = true
//            case e: Throwable => throw e // let other exceptions propagate
//        }
//        !hasSessionExpirationException},
//        s"Can't create topic $topic")
//
//        // wait until we've propagated all partitions metadata to all servers
//        val allPartitionsMetadata = waitForAllPartitionsMetadata(servers, topic, partitionReplicaAssignment.size)
//
//        partitionReplicaAssignment.keySet.map { i =>
//            i -> allPartitionsMetadata.get(new TopicPartition(topic, i)).map(_.leader()).getOrElse(
//            throw new IllegalStateException(s"Cannot get the partition leader for topic: $topic, partition: $i in server metadata cache"))
//        }.toMap
//    }
//
//    /**
//     * Create the consumer offsets/group metadata topic and wait until the leader is elected and metadata is propagated
//     * to all brokers.
//     */
//    def createOffsetsTopic(zkClient: KafkaZkClient, servers: Seq[KafkaBroker]): Unit = {
//        val server = servers.head
//        createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME,
//                server.config.getInt(KafkaConfig.OffsetsTopicPartitionsProp),
//                server.config.getShort(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
//                servers,
//                server.groupCoordinator.offsetsTopicConfigs)
//    }

    public static MemoryRecords singletonRecords(byte[] value) {
        return singletonRecords(value, null, CompressionType.NONE, RecordBatch.NO_TIMESTAMP, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(byte[] value, byte[] key) {
        return singletonRecords(value, key, CompressionType.NONE, RecordBatch.NO_TIMESTAMP, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(byte[] value,
                                                 Long timestamp) {
        return singletonRecords(value, null, CompressionType.NONE, timestamp, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(byte[] value,
                                                 byte[] key,
                                                 CompressionType codec,
                                                 Long timestamp) {
        return singletonRecords(value, key, codec, timestamp, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    /**
     * Wrap a single record log buffer.
     */
    public static MemoryRecords singletonRecords(byte[] value,
                                                 byte[] key,
                                                 CompressionType codec,
                                                 Long timestamp,
                                                 Byte magicValue) {
        return records(Arrays.asList(new SimpleRecord(timestamp, key, value)),
                magicValue,
                codec,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                0L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }
//
//    def recordsWithValues(magicValue: Byte,
//                          codec: CompressionType,
//                          values: Array[Byte]*): MemoryRecords = {
//        records(values.map(value => new SimpleRecord(value)), magicValue, codec)
//    }

    public static MemoryRecords records(Iterable<SimpleRecord> records,
                                        Byte magicValue,
                                        CompressionType codec,
                                        Long producerId,
                                        Short producerEpoch,
                                        Integer sequence,
                                        Long baseOffset,
                                        Integer partitionLeaderEpoch) {
        ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, baseOffset,
                System.currentTimeMillis(), producerId, producerEpoch, sequence, false, partitionLeaderEpoch);
        for (SimpleRecord record : records) {
            builder.append(record);
        }
        return builder.build();
    }
//
//    /**
//     * Generate an array of random bytes
//     *
//     * @param numBytes The size of the array
//     */
//    def randomBytes(numBytes: Int): Array[Byte] = JTestUtils.randomBytes(numBytes)
//
//    /**
//     * Generate a random string of letters and digits of the given length
//     *
//     * @param len The length of the string
//     * @return The random string
//     */
//    def randomString(len: Int): String = JTestUtils.randomString(len)
//
//    /**
//     * Check that the buffer content from buffer.position() to buffer.limit() is equal
//     */
//    def checkEquals(b1: ByteBuffer, b2: ByteBuffer): Unit = {
//        assertEquals(b1.limit() - b1.position(), b2.limit() - b2.position(), "Buffers should have equal length")
//        for(i <- 0 until b1.limit() - b1.position())
//        assertEquals(b1.get(b1.position() + i), b2.get(b1.position() + i), "byte " + i + " byte not equal.")
//    }
//
//    /**
//     *  Throw an exception if an iterable has different length than expected
//     *
//     */
//    def checkLength[T](s1: Iterator[T], expectedLength:Int): Unit = {
//        var n = 0
//        while (s1.hasNext) {
//            n += 1
//            s1.next()
//        }
//        assertEquals(expectedLength, n)
//    }
//
//    /**
//     * Throw an exception if the two iterators are of differing lengths or contain
//     * different messages on their Nth element
//     */
//    def checkEquals[T](s1: java.util.Iterator[T], s2: java.util.Iterator[T]): Unit = {
//        while(s1.hasNext && s2.hasNext)
//            assertEquals(s1.next, s2.next)
//        assertFalse(s1.hasNext, "Iterators have uneven length--first has more")
//        assertFalse(s2.hasNext, "Iterators have uneven length--second has more")
//    }
//
//    def stackedIterator[T](s: Iterator[T]*): Iterator[T] = {
//        new Iterator[T] {
//            var cur: Iterator[T] = null
//            val topIterator = s.iterator
//
//            def hasNext: Boolean = {
//            while (true) {
//                if (cur == null) {
//                    if (topIterator.hasNext)
//                        cur = topIterator.next()
//                    else
//                        return false
//                }
//                if (cur.hasNext)
//                    return true
//                cur = null
//            }
//            // should never reach here
//            throw new RuntimeException("should not reach here")
//      }
//
//            def next() : T = cur.next()
//        }
//    }
//
//    /**
//     * Create a hexadecimal string for the given bytes
//     */
//    def hexString(bytes: Array[Byte]): String = hexString(ByteBuffer.wrap(bytes))
//
//    /**
//     * Create a hexadecimal string for the given bytes
//     */
//    def hexString(buffer: ByteBuffer): String = {
//        val builder = new StringBuilder("0x")
//        for(i <- 0 until buffer.limit())
//        builder.append(String.format("%x", Integer.valueOf(buffer.get(buffer.position() + i))))
//        builder.toString
//    }
//
//    /**
//     * Returns security configuration options for broker or clients
//     *
//     * @param mode Client or server mode
//     * @param securityProtocol Security protocol which indicates if SASL or SSL or both configs are included
//     * @param trustStoreFile Trust store file must be provided for SSL and SASL_SSL
//     * @param certAlias Alias of certificate in SSL key store
//     * @param certCn CN for certificate
//     * @param saslProperties SASL configs if security protocol is SASL_SSL or SASL_PLAINTEXT
//     * @param tlsProtocol TLS version
//     * @param needsClientCert If not empty, a flag which indicates if client certificates are required. By default
//     *                        client certificates are generated only if securityProtocol is SSL (not for SASL_SSL).
//     */
//    def securityConfigs(mode: Mode,
//                        securityProtocol: SecurityProtocol,
//                        trustStoreFile: Option[File],
//                        certAlias: String,
//                        certCn: String,
//                        saslProperties: Option[Properties],
//                        tlsProtocol: String = TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS,
//                        needsClientCert: Option[Boolean] = None): Properties = {
//        val props = new Properties
//        if (usesSslTransportLayer(securityProtocol)) {
//            val addClientCert = needsClientCert.getOrElse(securityProtocol == SecurityProtocol.SSL)
//            props ++= sslConfigs(mode, addClientCert, trustStoreFile, certAlias, certCn, tlsProtocol)
//        }
//
//        if (usesSaslAuthentication(securityProtocol))
//            props ++= JaasTestUtils.saslConfigs(saslProperties)
//        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
//        props
//    }
//
//    def producerSecurityConfigs(securityProtocol: SecurityProtocol,
//                                trustStoreFile: Option[File],
//                                saslProperties: Option[Properties]): Properties =
//    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "producer", SslCertificateCn, saslProperties)
//
//    /**
//     * Create a (new) producer with a few pre-configured properties.
//     */
//    def createProducer[K, V](brokerList: String,
//    acks: Int = -1,
//    maxBlockMs: Long = 60 * 1000L,
//    bufferSize: Long = 1024L * 1024L,
//    retries: Int = Int.MaxValue,
//    deliveryTimeoutMs: Int = 30 * 1000,
//    lingerMs: Int = 0,
//    batchSize: Int = 16384,
//    compressionType: String = "none",
//    requestTimeoutMs: Int = 20 * 1000,
//    securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
//    trustStoreFile: Option[File] = None,
//    saslProperties: Option[Properties] = None,
//    keySerializer: Serializer[K] = new ByteArraySerializer,
//    valueSerializer: Serializer[V] = new ByteArraySerializer,
//    enableIdempotence: Boolean = false): KafkaProducer[K, V] = {
//        val producerProps = new Properties
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
//        producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
//        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
//        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
//        producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
//        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs.toString)
//        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString)
//        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
//        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
//        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType)
//        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)
//        producerProps ++= producerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
//        new KafkaProducer[K, V](producerProps, keySerializer, valueSerializer)
//    }

    public static Boolean usesSslTransportLayer(SecurityProtocol securityProtocol) {
        switch (securityProtocol) {
            case SSL:
            case SASL_SSL:
                return true;
            default:
                return false;
        }
    }

    public static Boolean usesSaslAuthentication(SecurityProtocol securityProtocol) {
        switch (securityProtocol) {
            case SASL_PLAINTEXT:
            case SASL_SSL:
                return true;
            default:
                return false;
        }
    }


//
//    def consumerSecurityConfigs(securityProtocol: SecurityProtocol, trustStoreFile: Option[File], saslProperties: Option[Properties]): Properties =
//    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "consumer", SslCertificateCn, saslProperties)
//
//    def adminClientSecurityConfigs(securityProtocol: SecurityProtocol, trustStoreFile: Option[File], saslProperties: Option[Properties]): Properties =
//    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "admin-client", SslCertificateCn, saslProperties)
//
//    /**
//     * Create a consumer with a few pre-configured properties.
//     */
//    def createConsumer[K, V](brokerList: String,
//    groupId: String = "group",
//    autoOffsetReset: String = "earliest",
//    enableAutoCommit: Boolean = true,
//    readCommitted: Boolean = false,
//    maxPollRecords: Int = 500,
//    securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
//    trustStoreFile: Option[File] = None,
//    saslProperties: Option[Properties] = None,
//    keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
//    valueDeserializer: Deserializer[V] = new ByteArrayDeserializer): KafkaConsumer[K, V] = {
//        val consumerProps = new Properties
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
//        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
//        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
//        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
//        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, if (readCommitted) "read_committed" else "read_uncommitted")
//        consumerProps ++= consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
//        new KafkaConsumer[K, V](consumerProps, keyDeserializer, valueDeserializer)
//    }
//
//    def createBrokersInZk(zkClient: KafkaZkClient, ids: Seq[Int]): Seq[Broker] =
//    createBrokersInZk(ids.map(kafka.admin.BrokerMetadata(_, None)), zkClient)
//
//    def createBrokersInZk(brokerMetadatas: Seq[kafka.admin.BrokerMetadata], zkClient: KafkaZkClient): Seq[Broker] = {
//        zkClient.makeSurePersistentPathExists(BrokerIdsZNode.path)
//        val brokers = brokerMetadatas.map { b =>
//            val protocol = SecurityProtocol.PLAINTEXT
//            val listenerName = ListenerName.forSecurityProtocol(protocol)
//            Broker(b.id, Seq(EndPoint("localhost", 6667, listenerName, protocol)), b.rack)
//        }
//        brokers.foreach(b => zkClient.registerBroker(BrokerInfo(Broker(b.id, b.endPoints, rack = b.rack),
//                MetadataVersion.latest, jmxPort = -1)))
//        brokers
//    }
//
//    def getMsgStrings(n: Int): Seq[String] = {
//        val buffer = new ListBuffer[String]
//        for (i <- 0 until  n)
//        buffer += ("msg" + i)
//        buffer
//    }
//
//    def makeLeaderForPartition(zkClient: KafkaZkClient,
//                               topic: String,
//                               leaderPerPartitionMap: scala.collection.immutable.Map[Int, Int],
//                               controllerEpoch: Int): Unit = {
//        val newLeaderIsrAndControllerEpochs = leaderPerPartitionMap.map { case (partition, leader) =>
//            val topicPartition = new TopicPartition(topic, partition)
//            val newLeaderAndIsr = zkClient.getTopicPartitionState(topicPartition)
//                    .map(_.leaderAndIsr.newLeader(leader))
//                    .getOrElse(LeaderAndIsr(leader, List(leader)))
//            topicPartition -> LeaderIsrAndControllerEpoch(newLeaderAndIsr, controllerEpoch)
//        }
//        zkClient.setTopicPartitionStatesRaw(newLeaderIsrAndControllerEpochs, ZkVersion.MatchAnyVersion)
//    }
//
//    /**
//     *  If neither oldLeaderOpt nor newLeaderOpt is defined, wait until the leader of a partition is elected.
//     *  If oldLeaderOpt is defined, it waits until the new leader is different from the old leader.
//     *  If newLeaderOpt is defined, it waits until the new leader becomes the expected new leader.
//     *
//     * @return The new leader (note that negative values are used to indicate conditions like NoLeader and
//     *         LeaderDuringDelete).
//     * @throws AssertionError if the expected condition is not true within the timeout.
//     */
//    def waitUntilLeaderIsElectedOrChanged(
//            zkClient: KafkaZkClient,
//            topic: String,
//            partition: Int,
//            timeoutMs: Long = 30000L,
//            oldLeaderOpt: Option[Int] = None,
//            newLeaderOpt: Option[Int] = None
//    ): Int = {
//        def getPartitionLeader(topic: String, partition: Int): Option[Int] = {
//                zkClient.getLeaderForPartition(new TopicPartition(topic, partition))
//        }
//        doWaitUntilLeaderIsElectedOrChanged(getPartitionLeader, topic, partition, timeoutMs, oldLeaderOpt, newLeaderOpt)
//    }
//
//    /**
//     *  If neither oldLeaderOpt nor newLeaderOpt is defined, wait until the leader of a partition is elected.
//     *  If oldLeaderOpt is defined, it waits until the new leader is different from the old leader.
//     *  If newLeaderOpt is defined, it waits until the new leader becomes the expected new leader.
//     *
//     * @return The new leader (note that negative values are used to indicate conditions like NoLeader and
//     *         LeaderDuringDelete).
//     * @throws AssertionError if the expected condition is not true within the timeout.
//     */
//    def waitUntilLeaderIsElectedOrChangedWithAdmin(
//            admin: Admin,
//            topic: String,
//            partition: Int,
//            timeoutMs: Long = 30000L,
//            oldLeaderOpt: Option[Int] = None,
//            newLeaderOpt: Option[Int] = None
//    ): Int = {
//        def getPartitionLeader(topic: String, partition: Int): Option[Int] = {
//                admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get().get(topic).partitions().asScala.
//                        find(_.partition() == partition).
//                        flatMap { p =>
//        if (p.leader().id() == Node.noNode().id()) {
//            None
//        } else {
//            Some(p.leader().id())
//        }
//        }
//    }
//        doWaitUntilLeaderIsElectedOrChanged(getPartitionLeader, topic, partition, timeoutMs, oldLeaderOpt, newLeaderOpt)
//    }
//
//    private def doWaitUntilLeaderIsElectedOrChanged(
//            getPartitionLeader: (String, Int) => Option[Int],
//    topic: String,
//    partition: Int,
//    timeoutMs: Long,
//    oldLeaderOpt: Option[Int],
//    newLeaderOpt: Option[Int]
//            ): Int = {
//        require(!(oldLeaderOpt.isDefined && newLeaderOpt.isDefined), "Can't define both the old and the new leader")
//        val startTime = System.currentTimeMillis()
//        val topicPartition = new TopicPartition(topic, partition)
//
//        trace(s"Waiting for leader to be elected or changed for partition $topicPartition, old leader is $oldLeaderOpt, " +
//                s"new leader is $newLeaderOpt")
//
//        var leader: Option[Int] = None
//        var electedOrChangedLeader: Option[Int] = None
//        while (electedOrChangedLeader.isEmpty && System.currentTimeMillis() < startTime + timeoutMs) {
//            // check if leader is elected
//            leader = getPartitionLeader(topic, partition)
//            leader match {
//                case Some(l) => (newLeaderOpt, oldLeaderOpt) match {
//                    case (Some(newLeader), _) if newLeader == l =>
//                        trace(s"Expected new leader $l is elected for partition $topicPartition")
//                        electedOrChangedLeader = leader
//                    case (_, Some(oldLeader)) if oldLeader != l =>
//                        trace(s"Leader for partition $topicPartition is changed from $oldLeader to $l")
//                        electedOrChangedLeader = leader
//                    case (None, None) =>
//                        trace(s"Leader $l is elected for partition $topicPartition")
//                        electedOrChangedLeader = leader
//                    case _ =>
//                        trace(s"Current leader for partition $topicPartition is $l")
//                }
//                case None =>
//                    trace(s"Leader for partition $topicPartition is not elected yet")
//            }
//            Thread.sleep(math.min(timeoutMs, 100L))
//        }
//        electedOrChangedLeader.getOrElse {
//            val errorMessage = (newLeaderOpt, oldLeaderOpt) match {
//                case (Some(newLeader), _) =>
//                    s"Timing out after $timeoutMs ms since expected new leader $newLeader was not elected for partition $topicPartition, leader is $leader"
//                case (_, Some(oldLeader)) =>
//                    s"Timing out after $timeoutMs ms since a new leader that is different from $oldLeader was not elected for partition $topicPartition, " +
//                        s"leader is $leader"
//                case _ =>
//                    s"Timing out after $timeoutMs ms since a leader was not elected for partition $topicPartition"
//            }
//            throw new AssertionError(errorMessage)
//        }
//    }
//
//    /**
//     * Execute the given block. If it throws an assert error, retry. Repeat
//     * until no error is thrown or the time limit elapses
//     */
//    def retry(maxWaitMs: Long)(block: => Unit): Unit = {
//        var wait = 1L
//        val startTime = System.currentTimeMillis()
//        while(true) {
//            try {
//                block
//                return
//            } catch {
//                case e: AssertionError =>
//                    val elapsed = System.currentTimeMillis - startTime
//                    if (elapsed > maxWaitMs) {
//                        throw e
//                    } else {
//                        info("Attempt failed, sleeping for " + wait + ", and then retrying.")
//                        Thread.sleep(wait)
//                        wait += math.min(wait, 1000)
//                    }
//            }
//        }
//    }
//
//    def pollUntilTrue(consumer: Consumer[_, _],
//                      action: () => Boolean,
//    msg: => String,
//    waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
//        waitUntilTrue(() => {
//                consumer.poll(Duration.ofMillis(100))
//                action()
//        }, msg = msg, pause = 0L, waitTimeMs = waitTimeMs)
//    }
//
//    def pollRecordsUntilTrue[K, V](consumer: Consumer[K, V],
//    action: ConsumerRecords[K, V] => Boolean,
//    msg: => String,
//    waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
//        waitUntilTrue(() => {
//                val records = consumer.poll(Duration.ofMillis(100))
//                action(records)
//        }, msg = msg, pause = 0L, waitTimeMs = waitTimeMs)
//    }
//
//    def subscribeAndWaitForRecords(topic: String,
//                                   consumer: KafkaConsumer[Array[Byte], Array[Byte]],
//                                   waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
//        consumer.subscribe(Collections.singletonList(topic))
//        pollRecordsUntilTrue(
//                consumer,
//                (records: ConsumerRecords[Array[Byte], Array[Byte]]) => !records.isEmpty,
//                "Expected records",
//                waitTimeMs)
//    }
//
//    /**
//     * Wait for the presence of an optional value.
//     *
//     * @param func The function defining the optional value
//     * @param msg Error message in the case that the value never appears
//     * @param waitTimeMs Maximum time to wait
//     * @return The unwrapped value returned by the function
//     */
//    def awaitValue[T](func: () => Option[T], msg: => String, waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): T = {
//        var value: Option[T] = None
//        waitUntilTrue(() => {
//                value = func()
//                value.isDefined
//        }, msg, waitTimeMs)
//        value.get
//    }

    /**
     * Wait until the given condition is true or throw an exception if the given wait time elapses.
     *
     * @param condition  condition to check
     * @param msg        error message
     * @param waitTimeMs maximum time to wait and retest the condition before failing the test
     * @param pause      delay between condition checks
     */
    public static void waitUntilTrue(Supplier<Boolean> condition,
                                     String msg,
                                     Long waitTimeMs,
                                     Long pause) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (condition.get()) {
                return;
            }
            if (System.currentTimeMillis() > startTime + waitTimeMs) {
                fail(msg);
            }
            Thread.sleep(Math.min(waitTimeMs, pause));
        }

        // should never hit here
//        throw new RuntimeException("unexpected error");
    }

//    /**
//     * Invoke `compute` until `predicate` is true or `waitTime` elapses.
//     *
//     * Return the last `compute` result and a boolean indicating whether `predicate` succeeded for that value.
//     *
//     * This method is useful in cases where `waitUntilTrue` makes it awkward to provide good error messages.
//     */
//    def computeUntilTrue[T](compute: => T, waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS, pause: Long = 100L)(
//    predicate: T => Boolean): (T, Boolean) = {
//        val startTime = System.currentTimeMillis()
//        while (true) {
//            val result = compute
//            if (predicate(result))
//                return result -> true
//            if (System.currentTimeMillis() > startTime + waitTime)
//                return result -> false
//            Thread.sleep(waitTime.min(pause))
//        }
//        // should never hit here
//        throw new RuntimeException("unexpected error")
//    }
//
//    /**
//     * Invoke `assertions` until no AssertionErrors are thrown or `waitTime` elapses.
//     *
//     * This method is useful in cases where there may be some expected delay in a particular test condition that is
//     * otherwise difficult to poll for. `computeUntilTrue` and `waitUntilTrue` should be preferred in cases where we can
//     * easily wait on a condition before evaluating the assertions.
//     */
//    def tryUntilNoAssertionError[T](waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS, pause: Long = 100L)(assertions: => T): T = {
//        val (either, success) = TestUtils.computeUntilTrue({
//        try {
//            val res = assertions
//            Left(res)
//        } catch {
//            case ae: AssertionError => Right(ae)
//        }
//    }, waitTime = waitTime, pause = pause)(_.isLeft)
//
//                either match {
//            case Left(res) => res
//            case Right(err) => throw err
//        }
//    }
//
//    def isLeaderLocalOnBroker(topic: String, partitionId: Int, broker: KafkaBroker): Boolean = {
//        broker.replicaManager.onlinePartition(new TopicPartition(topic, partitionId)).exists(_.leaderLogIfLocal.isDefined)
//    }
//
//    def findLeaderEpoch(brokerId: Int,
//                        topicPartition: TopicPartition,
//                        brokers: Iterable[KafkaBroker]): Int = {
//        val leaderBroker = brokers.find(_.config.brokerId == brokerId)
//        val leaderPartition = leaderBroker.flatMap(_.replicaManager.onlinePartition(topicPartition))
//                .getOrElse(throw new AssertionError(s"Failed to find expected replica on broker $brokerId"))
//        leaderPartition.getLeaderEpoch
//    }
//
//    def findFollowerId(topicPartition: TopicPartition,
//                       brokers: Iterable[KafkaBroker]): Int = {
//        val followerOpt = brokers.find { server =>
//            server.replicaManager.onlinePartition(topicPartition) match {
//                case Some(partition) => !partition.leaderReplicaIdOpt.contains(server.config.brokerId)
//                case None => false
//            }
//        }
//        followerOpt
//                .map(_.config.brokerId)
//                .getOrElse(throw new AssertionError(s"Unable to locate follower for $topicPartition"))
//    }
//
//    /**
//     * Wait until all brokers know about each other.
//     *
//     * @param brokers The Kafka brokers.
//     * @param timeout The amount of time waiting on this condition before assert to fail
//     */
//    def waitUntilBrokerMetadataIsPropagated[B <: KafkaBroker](
//    brokers: Seq[B],
//    timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
//        val expectedBrokerIds = brokers.map(_.config.brokerId).toSet
//        waitUntilTrue(() => brokers.forall(server =>
//                expectedBrokerIds == server.dataPlaneRequestProcessor.metadataCache.getAliveBrokers().map(_.id).toSet
//    ), "Timed out waiting for broker metadata to propagate to all servers", timeout)
//    }
//
//    /**
//     * Wait until the expected number of partitions is in the metadata cache in each broker.
//     *
//     * @param brokers The list of brokers that the metadata should reach to
//     * @param topic The topic name
//     * @param expectedNumPartitions The expected number of partitions
//     * @return all partitions metadata
//     */
//    def waitForAllPartitionsMetadata[B <: KafkaBroker](
//    brokers: Seq[B],
//    topic: String,
//    expectedNumPartitions: Int): Map[TopicPartition, UpdateMetadataPartitionState] = {
//        waitUntilTrue(
//                () => brokers.forall { broker =>
//            if (expectedNumPartitions == 0) {
//                broker.metadataCache.numPartitions(topic) == None
//            } else {
//                broker.metadataCache.numPartitions(topic) == Some(expectedNumPartitions)
//            }
//        },
//        s"Topic [$topic] metadata not propagated after 60000 ms", waitTimeMs = 60000L)
//
//        // since the metadata is propagated, we should get the same metadata from each server
//        (0 until expectedNumPartitions).map { i =>
//            new TopicPartition(topic, i) -> brokers.head.metadataCache.getPartitionInfo(topic, i).getOrElse(
//            throw new IllegalStateException(s"Cannot get topic: $topic, partition: $i in server metadata cache"))
//        }.toMap
//    }
//
//    /**
//     * Wait until a valid leader is propagated to the metadata cache in each broker.
//     * It assumes that the leader propagated to each broker is the same.
//     *
//     * @param brokers The list of brokers that the metadata should reach to
//     * @param topic The topic name
//     * @param partition The partition Id
//     * @param timeout The amount of time waiting on this condition before assert to fail
//     * @return The metadata of the partition.
//     */
//    def waitForPartitionMetadata[B <: KafkaBroker](
//    brokers: Seq[B], topic: String, partition: Int,
//    timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): UpdateMetadataPartitionState = {
//        waitUntilTrue(
//                () => brokers.forall { broker =>
//            broker.metadataCache.getPartitionInfo(topic, partition) match {
//                case Some(partitionState) => Request.isValidBrokerId(partitionState.leader)
//                case _ => false
//            }
//        },
//        "Partition [%s,%d] metadata not propagated after %d ms".format(topic, partition, timeout),
//                waitTimeMs = timeout)
//
//        brokers.head.metadataCache.getPartitionInfo(topic, partition).getOrElse(
//        throw new IllegalStateException(s"Cannot get topic: $topic, partition: $partition in server metadata cache"))
//    }
//
//    /**
//     * Wait until the kraft broker metadata have caught up to the controller, before calling this, we should make sure
//     * the related metadata message has already been committed to the controller metadata log.
//     */
//    def ensureConsistentKRaftMetadata(
//            brokers: Seq[KafkaBroker],
//            controllerServer: ControllerServer,
//            msg: String = "Timeout waiting for controller metadata propagating to brokers"
//    ): Unit = {
//        val controllerOffset = controllerServer.raftManager.replicatedLog.endOffset().offset - 1
//        TestUtils.waitUntilTrue(
//                () => {
//                brokers.forall { broker =>
//                val metadataOffset = broker.asInstanceOf[BrokerServer].metadataPublisher.publishedOffset
//                metadataOffset >= controllerOffset
//        }
//      }, msg)
//    }
//
//    def waitUntilControllerElected(zkClient: KafkaZkClient, timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
//        val (controllerId, _) = computeUntilTrue(zkClient.getControllerId, waitTime = timeout)(_.isDefined)
//                controllerId.getOrElse(throw new AssertionError(s"Controller not elected after $timeout ms"))
//    }
//
//    def awaitLeaderChange[B <: KafkaBroker](
//    brokers: Seq[B],
//    tp: TopicPartition,
//    oldLeader: Int,
//    timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
//        def newLeaderExists: Option[Int] = {
//                brokers.find { broker =>
//                broker.config.brokerId != oldLeader &&
//                broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
//        }.map(_.config.brokerId)
//    }
//
//        waitUntilTrue(() => newLeaderExists.isDefined,
//                s"Did not observe leader change for partition $tp after $timeout ms", waitTimeMs = timeout)
//
//        newLeaderExists.get
//    }
//
//    def waitUntilLeaderIsKnown[B <: KafkaBroker](
//    brokers: Seq[B],
//    tp: TopicPartition,
//    timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
//        def leaderIfExists: Option[Int] = {
//                brokers.find { broker =>
//                broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
//        }.map(_.config.brokerId)
//    }
//
//        waitUntilTrue(() => leaderIfExists.isDefined,
//                s"Partition $tp leaders not made yet after $timeout ms", waitTimeMs = timeout)
//
//        leaderIfExists.get
//    }
//
//    def writeNonsenseToFile(fileName: File, position: Long, size: Int): Unit = {
//        val file = new RandomAccessFile(fileName, "rw")
//        file.seek(position)
//        for (_ <- 0 until size)
//        file.writeByte(random.nextInt(255))
//        file.close()
//    }
//
//    def appendNonsenseToFile(file: File, size: Int): Unit = {
//        val outputStream = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND)
//        try {
//            for (_ <- 0 until size)
//            outputStream.write(random.nextInt(255))
//        } finally outputStream.close()
//    }
//
//    def checkForPhantomInSyncReplicas(zkClient: KafkaZkClient, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int]): Unit = {
//        val inSyncReplicas = zkClient.getInSyncReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
//        // in sync replicas should not have any replica that is not in the new assigned replicas
//        val phantomInSyncReplicas = inSyncReplicas.get.toSet -- assignedReplicas.toSet
//        assertTrue(phantomInSyncReplicas.isEmpty,
//                "All in sync replicas %s must be in the assigned replica list %s".format(inSyncReplicas, assignedReplicas))
//    }
//
//    def ensureNoUnderReplicatedPartitions(zkClient: KafkaZkClient, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int],
//                                          servers: Seq[KafkaServer]): Unit = {
//        val topicPartition = new TopicPartition(topic, partitionToBeReassigned)
//        waitUntilTrue(() => {
//                val inSyncReplicas = zkClient.getInSyncReplicasForPartition(topicPartition)
//                inSyncReplicas.get.size == assignedReplicas.size
//        },
//                "Reassigned partition [%s,%d] is under replicated".format(topic, partitionToBeReassigned))
//        var leader: Option[Int] = None
//        waitUntilTrue(() => {
//                leader = zkClient.getLeaderForPartition(topicPartition)
//                leader.isDefined
//        },
//                "Reassigned partition [%s,%d] is unavailable".format(topic, partitionToBeReassigned))
//        waitUntilTrue(() => {
//                val leaderBroker = servers.filter(s => s.config.brokerId == leader.get).head
//        leaderBroker.replicaManager.underReplicatedPartitionCount == 0
//      },
//        "Reassigned partition [%s,%d] is under-replicated as reported by the leader %d".format(topic, partitionToBeReassigned, leader.get))
//    }
//
//    // Note: Call this method in the test itself, rather than the @AfterEach method.
//    // Because of the assert, if assertNoNonDaemonThreads fails, nothing after would be executed.
//    def assertNoNonDaemonThreads(threadNamePrefix: String): Unit = {
//        val threadCount = Thread.getAllStackTraces.keySet.asScala.count { t =>
//            !t.isDaemon && t.isAlive && t.getName.startsWith(threadNamePrefix)
//        }
//        assertEquals(0, threadCount)
//    }
//
//    def allThreadStackTraces(): String = {
//        Thread.getAllStackTraces.asScala.map { case (thread, stackTrace) =>
//            thread.getName + "\n\t" + stackTrace.toList.map(_.toString).mkString("\n\t")
//        }.mkString("\n")
//    }

    public static LogManager createLogManager(List<File> logDirs) throws Throwable {
        return createLogManager(logDirs,
                LogConfig.apply(),
                new MockConfigRepository(),
                new CleanerConfig(
                        CleanerConfig.Defaults.NUM_THREADS,
                        CleanerConfig.Defaults.DEDUPE_BUFFER_SIZE,
                        CleanerConfig.Defaults.DEDUPE_BUFFER_LOAD_FACTOR,
                        CleanerConfig.Defaults.IO_BUFFER_SIZE,
                        CleanerConfig.Defaults.MAX_MESSAGE_SIZE,
                        CleanerConfig.Defaults.MAX_IO_BYTES_PER_SECOND,
                        CleanerConfig.Defaults.BACK_OFF_MS,
                        false,
                        CleanerConfig.Defaults.HASH_ALGORITHM),
                new MockTime(),
                MetadataVersion.latest(),
                4);
    }

    /**
     * Create new LogManager instance with default configuration for testing
     */
    public static LogManager createLogManager(List<File> logDirs,
                                              LogConfig defaultConfig,
                                              ConfigRepository configRepository,
                                              CleanerConfig cleanerConfig,
                                              MockTime time,
                                              MetadataVersion interBrokerProtocolVersion,
                                              Integer recoveryThreadsPerDataDir) throws IOException {

        return new LogManager(logDirs.stream().map(File::getAbsoluteFile).collect(Collectors.toList()),
                new ArrayList<>(),
                configRepository,
                defaultConfig,
                cleanerConfig,
                recoveryThreadsPerDataDir,
                1000L,
                10000L,
                10000L,
                1000L,
                5 * 60 * 1000,
                60 * 60 * 1000,
                interBrokerProtocolVersion,
                time.getScheduler(),
                new BrokerTopicStats(),
                new LogDirFailureChannel(logDirs.size()),
                time,
                true);
    }
//
//    class MockAlterPartitionManager extends AlterPartitionManager {
//        val isrUpdates: mutable.Queue[AlterPartitionItem] = new mutable.Queue[AlterPartitionItem]()
//        val inFlight: AtomicBoolean = new AtomicBoolean(false)
//
//
//        override def submit(
//                topicPartition: TopicIdPartition,
//                leaderAndIsr: LeaderAndIsr,
//                controllerEpoch: Int
//        ): CompletableFuture[LeaderAndIsr]= {
//            val future = new CompletableFuture[LeaderAndIsr]()
//            if (inFlight.compareAndSet(false, true)) {
//                isrUpdates += AlterPartitionItem(
//                        topicPartition,
//                        leaderAndIsr,
//                        future,
//                        controllerEpoch
//                )
//            } else {
//                future.completeExceptionally(new OperationNotAttemptedException(
//                        s"Failed to enqueue AlterIsr request for $topicPartition since there is already an inflight request"))
//            }
//            future
//        }
//
//        def completeIsrUpdate(newPartitionEpoch: Int): Unit = {
//            if (inFlight.compareAndSet(true, false)) {
//                val item = isrUpdates.dequeue()
//                item.future.complete(item.leaderAndIsr.withPartitionEpoch(newPartitionEpoch))
//            } else {
//                fail("Expected an in-flight ISR update, but there was none")
//            }
//        }
//
//        def failIsrUpdate(error: Errors): Unit = {
//            if (inFlight.compareAndSet(true, false)) {
//                val item = isrUpdates.dequeue()
//                item.future.completeExceptionally(error.exception)
//            } else {
//                fail("Expected an in-flight ISR update, but there was none")
//            }
//        }
//    }
//
//    def createAlterIsrManager(): MockAlterPartitionManager = {
//        new MockAlterPartitionManager()
//    }
//
//    class MockAlterPartitionListener extends AlterPartitionListener {
//        val expands: AtomicInteger = new AtomicInteger(0)
//        val shrinks: AtomicInteger = new AtomicInteger(0)
//        val failures: AtomicInteger = new AtomicInteger(0)
//
//        override def markIsrExpand(): Unit = expands.incrementAndGet()
//
//        override def markIsrShrink(): Unit = shrinks.incrementAndGet()
//
//        override def markFailed(): Unit = failures.incrementAndGet()
//
//        def reset(): Unit = {
//            expands.set(0)
//            shrinks.set(0)
//            failures.set(0)
//        }
//    }
//
//    def createIsrChangeListener(): MockAlterPartitionListener = {
//        new MockAlterPartitionListener()
//    }
//
//    def produceMessages[B <: KafkaBroker](
//    brokers: Seq[B],
//    records: Seq[ProducerRecord[Array[Byte], Array[Byte]]],
//    acks: Int = -1): Unit = {
//        val producer = createProducer(plaintextBootstrapServers(brokers), acks = acks)
//        try {
//            val futures = records.map(producer.send)
//            futures.foreach(_.get)
//        } finally {
//            producer.close()
//        }
//
//        val topics = records.map(_.topic).distinct
//        debug(s"Sent ${records.size} messages for topics ${topics.mkString(",")}")
//    }
//
//    def generateAndProduceMessages[B <: KafkaBroker](
//    brokers: Seq[B],
//    topic: String,
//    numMessages: Int,
//    acks: Int = -1): Seq[String] = {
//        val values = (0 until numMessages).map(x =>  s"test-$x")
//        val intSerializer = new IntegerSerializer()
//        val records = values.zipWithIndex.map { case (v, i) =>
//            new ProducerRecord(topic, intSerializer.serialize(topic, i), v.getBytes)
//        }
//        produceMessages(brokers, records, acks)
//        values
//    }
//
//    def produceMessage[B <: KafkaBroker](
//    brokers: Seq[B],
//    topic: String,
//    message: String,
//    timestamp: java.lang.Long = null,
//    deliveryTimeoutMs: Int = 30 * 1000,
//    requestTimeoutMs: Int = 20 * 1000): Unit = {
//        val producer = createProducer(plaintextBootstrapServers(brokers),
//                deliveryTimeoutMs = deliveryTimeoutMs, requestTimeoutMs = requestTimeoutMs)
//        try {
//            producer.send(new ProducerRecord(topic, null, timestamp, topic.getBytes, message.getBytes)).get
//        } finally {
//            producer.close()
//        }
//    }
//
//    def verifyTopicDeletion[B <: KafkaBroker](
//    zkClient: KafkaZkClient,
//    topic: String,
//    numPartitions: Int,
//    brokers: Seq[B]): Unit = {
//        val topicPartitions = (0 until numPartitions).map(new TopicPartition(topic, _))
//        if (zkClient != null) {
//            // wait until admin path for delete topic is deleted, signaling completion of topic deletion
//            waitUntilTrue(() => !zkClient.isTopicMarkedForDeletion(topic),
//                    "Admin path /admin/delete_topics/%s path not deleted even after a replica is restarted".format(topic))
//            waitUntilTrue(() => !zkClient.topicExists(topic),
//                    "Topic path /brokers/topics/%s not deleted after /admin/delete_topics/%s path is deleted".format(topic, topic))
//        }
//        // ensure that the topic-partition has been deleted from all brokers' replica managers
//        waitUntilTrue(() =>
//                brokers.forall(broker => topicPartitions.forall(tp => broker.replicaManager.onlinePartition(tp).isEmpty)),
//        "Replica manager's should have deleted all of this topic's partitions")
//        // ensure that logs from all replicas are deleted if delete topic is marked successful in ZooKeeper
//        assertTrue(brokers.forall(broker => topicPartitions.forall(tp => broker.logManager.getLog(tp).isEmpty)),
//        "Replica logs not deleted after delete topic is complete")
//        // ensure that topic is removed from all cleaner offsets
//        waitUntilTrue(() => brokers.forall(broker => topicPartitions.forall { tp =>
//            val checkpoints = broker.logManager.liveLogDirs.map { logDir =>
//                new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint")).read()
//            }
//            checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.contains(tp))
//        }), "Cleaner offset for deleted partition should have been removed")
//        waitUntilTrue(() => brokers.forall(broker =>
//                broker.config.logDirs.forall { logDir =>
//            topicPartitions.forall { tp =>
//                !new File(logDir, tp.topic + "-" + tp.partition).exists()
//            }
//        }
//    ), "Failed to soft-delete the data to a delete directory")
//        waitUntilTrue(() => brokers.forall(broker =>
//                broker.config.logDirs.forall { logDir =>
//            topicPartitions.forall { tp =>
//                !Arrays.asList(new File(logDir).list()).asScala.exists { partitionDirectoryName =>
//                    partitionDirectoryName.startsWith(tp.topic + "-" + tp.partition) &&
//                            partitionDirectoryName.endsWith(UnifiedLog.DeleteDirSuffix)
//                }
//            }
//        }
//    ), "Failed to hard-delete the delete directory")
//    }
//
//
//    def causeLogDirFailure(failureType: LogDirFailureType, leaderBroker: KafkaBroker, partition: TopicPartition): Unit = {
//        // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
//        val localLog = leaderBroker.replicaManager.localLogOrException(partition)
//        val logDir = localLog.dir.getParentFile
//        CoreUtils.swallow(Utils.delete(logDir), this)
//        Files.createFile(logDir.toPath)
//        assertTrue(logDir.isFile)
//
//        if (failureType == Roll) {
//            assertThrows(classOf[KafkaStorageException], () => leaderBroker.replicaManager.getLog(partition).get.roll())
//        } else if (failureType == Checkpoint) {
//            leaderBroker.replicaManager.checkpointHighWatermarks()
//        }
//
//        // Wait for ReplicaHighWatermarkCheckpoint to happen so that the log directory of the topic will be offline
//        waitUntilTrue(() => !leaderBroker.logManager.isLogDirOnline(logDir.getAbsolutePath), "Expected log directory offline", 3000L)
//        assertTrue(leaderBroker.replicaManager.localLog(partition).isEmpty)
//    }
//

    public static String readString(ByteBuffer buffer) throws UnsupportedEncodingException {
        return readString(buffer, Charset.defaultCharset().toString());
    }

    /**
     * Translate the given buffer into a string
     *
     * @param buffer   The buffer to translate
     * @param encoding The encoding to use in translating bytes to characters
     */
    public static String readString(ByteBuffer buffer, String encoding) throws UnsupportedEncodingException {
        byte[] bytes = new byte[(buffer.remaining())];
        buffer.get(bytes);
        return new String(bytes, encoding);
    }
//
//    def copyOf(props: Properties): Properties = {
//        val copy = new Properties()
//        copy ++= props
//        copy
//    }

    public static Properties sslConfigs(Mode mode,
                                        Boolean clientCert,
                                        Optional<File> trustStoreFile,
                                        String certAlias) throws Exception {
        return sslConfigs(mode, clientCert, trustStoreFile, certAlias, SslCertificateCn, TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS);
    }

    public static Properties sslConfigs(Mode mode,
                                        Boolean clientCert,
                                        Optional<File> trustStoreFile,
                                        String certAlias,
                                        String certCn,
                                        String tlsProtocol) throws Exception {
        if (!trustStoreFile.isPresent()) {
            throw new Exception("SSL enabled but no trustStoreFile provided");
        }
        File trustStore = trustStoreFile.get();

        Map<String, Object> sslConfigs = new TestSslUtils.SslConfigsBuilder(mode)
                .useClientCert(clientCert)
                .createNewTrustStore(trustStore)
                .certAlias(certAlias)
                .cn(certCn)
                .tlsProtocol(tlsProtocol)
                .build();

        Properties sslProps = new Properties();
        sslProps.putAll(sslConfigs);
        return sslProps;
    }

//    // a X509TrustManager to trust self-signed certs for unit tests.
//    def trustAllCerts: X509TrustManager = {
//        val trustManager = new X509TrustManager() {
//            override def getAcceptedIssuers: Array[X509Certificate] = {
//                null
//            }
//            override def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {
//            }
//            override def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {
//            }
//        }
//        trustManager
//    }
//
//    def waitAndVerifyAcls(expected: Set[AccessControlEntry],
//                          authorizer: JAuthorizer,
//                          resource: ResourcePattern,
//                          accessControlEntryFilter: AccessControlEntryFilter = AccessControlEntryFilter.ANY): Unit = {
//        val newLine = scala.util.Properties.lineSeparator
//
//        val filter = new AclBindingFilter(resource.toFilter, accessControlEntryFilter)
//        waitUntilTrue(() => authorizer.acls(filter).asScala.map(_.entry).toSet == expected,
//                s"expected acls:${expected.mkString(newLine + "\t", newLine + "\t", newLine)}" +
//                s"but got:${authorizer.acls(filter).asScala.map(_.entry).mkString(newLine + "\t", newLine + "\t", newLine)}",
//                45000)
//    }
//
//    /**
//     * Verifies that this ACL is the secure one.
//     */
//    def isAclSecure(acl: ACL, sensitive: Boolean): Boolean = {
//        debug(s"ACL $acl")
//        acl.getPerms match {
//            case Perms.READ => !sensitive && acl.getId.getScheme == "world"
//            case Perms.ALL => acl.getId.getScheme == "sasl"
//            case _ => false
//        }
//    }
//
//    /**
//     * Verifies that the ACL corresponds to the unsecure one that
//     * provides ALL access to everyone (world).
//     */
//    def isAclUnsecure(acl: ACL): Boolean = {
//        debug(s"ACL $acl")
//        acl.getPerms match {
//            case Perms.ALL => acl.getId.getScheme == "world"
//            case _ => false
//        }
//    }
//
//    private def secureZkPaths(zkClient: KafkaZkClient): Seq[String] = {
//        def subPaths(path: String): Seq[String] = {
//        if (zkClient.pathExists(path))
//            path +: zkClient.getChildren(path).map(c => path + "/" + c).flatMap(subPaths)
//      else
//        Seq.empty
//    }
//        val topLevelPaths = ZkData.SecureRootPaths ++ ZkData.SensitiveRootPaths
//        topLevelPaths.flatMap(subPaths)
//    }
//
//    /**
//     * Verifies that all secure paths in ZK are created with the expected ACL.
//     */
//    def verifySecureZkAcls(zkClient: KafkaZkClient, usersWithAccess: Int): Unit = {
//        secureZkPaths(zkClient).foreach(path => {
//        if (zkClient.pathExists(path)) {
//            val sensitive = ZkData.sensitivePath(path)
//            // usersWithAccess have ALL access to path. For paths that are
//            // not sensitive, world has READ access.
//            val aclCount = if (sensitive) usersWithAccess else usersWithAccess + 1
//            val acls = zkClient.getAcl(path)
//            assertEquals(aclCount, acls.size, s"Invalid ACLs for $path $acls")
//            acls.foreach(acl => isAclSecure(acl, sensitive))
//        }
//    })
//    }
//
//    /**
//     * Verifies that secure paths in ZK have no access control. This is
//     * the case when zookeeper.set.acl=false and no ACLs have been configured.
//     */
//    def verifyUnsecureZkAcls(zkClient: KafkaZkClient): Unit = {
//        secureZkPaths(zkClient).foreach(path => {
//        if (zkClient.pathExists(path)) {
//            val acls = zkClient.getAcl(path)
//            assertEquals(1, acls.size, s"Invalid ACLs for $path $acls")
//            acls.foreach(isAclUnsecure)
//        }
//    })
//    }
//
//    /**
//     * To use this you pass in a sequence of functions that are your arrange/act/assert test on the SUT.
//     * They all run at the same time in the assertConcurrent method; the chances of triggering a multithreading code error,
//     * and thereby failing some assertion are greatly increased.
//     */
//    def assertConcurrent(message: String, functions: Seq[() => Any], timeoutMs: Int): Unit = {
//
//        def failWithTimeout(): Unit = {
//                fail(s"$message. Timed out, the concurrent functions took more than $timeoutMs milliseconds")
//        }
//
//        val numThreads = functions.size
//        val threadPool = Executors.newFixedThreadPool(numThreads)
//        val exceptions = ArrayBuffer[Throwable]()
//        try {
//            val runnables = functions.map { function =>
//                new Callable[Unit] {
//                    override def call(): Unit = function()
//                }
//            }.asJava
//            val futures = threadPool.invokeAll(runnables, timeoutMs, TimeUnit.MILLISECONDS).asScala
//            futures.foreach { future =>
//                if (future.isCancelled)
//                    failWithTimeout()
//                else
//                    try future.get()
//          catch { case e: Exception =>
//                    exceptions += e
//                }
//            }
//        } catch {
//            case _: InterruptedException => failWithTimeout()
//            case e: Throwable => exceptions += e
//        } finally {
//            threadPool.shutdownNow()
//        }
//        assertTrue(exceptions.isEmpty, s"$message failed with exception(s) $exceptions")
//    }
//
//    def consumeTopicRecords[K, V, B <: KafkaBroker](
//    brokers: Seq[B],
//    topic: String,
//    numMessages: Int,
//    groupId: String = "group",
//    securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
//    trustStoreFile: Option[File] = None,
//    waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
//        val consumer = createConsumer(bootstrapServers(brokers, ListenerName.forSecurityProtocol(securityProtocol)),
//                groupId = groupId,
//                securityProtocol = securityProtocol,
//                trustStoreFile = trustStoreFile)
//        try {
//            consumer.subscribe(Collections.singleton(topic))
//            consumeRecords(consumer, numMessages, waitTime)
//        } finally consumer.close()
//    }
//
//    def pollUntilAtLeastNumRecords[K, V](consumer: Consumer[K, V],
//    numRecords: Int,
//    waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
//        val records = new ArrayBuffer[ConsumerRecord[K, V]]()
//        def pollAction(polledRecords: ConsumerRecords[K, V]): Boolean = {
//                records ++= polledRecords.asScala
//                records.size >= numRecords
//        }
//        pollRecordsUntilTrue(consumer, pollAction,
//                waitTimeMs = waitTimeMs,
//                msg = s"Consumed ${records.size} records before timeout instead of the expected $numRecords records")
//        records
//    }
//
//    def consumeRecords[K, V](consumer: Consumer[K, V],
//    numRecords: Int,
//    waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
//        val records = pollUntilAtLeastNumRecords(consumer, numRecords, waitTimeMs)
//        assertEquals(numRecords, records.size, "Consumed more records than expected")
//        records
//    }
//
//    /**
//     * Will consume all the records for the given consumer for the specified duration. If you want to drain all the
//     * remaining messages in the partitions the consumer is subscribed to, the duration should be set high enough so
//     * that the consumer has enough time to poll everything. This would be based on the number of expected messages left
//     * in the topic, and should not be too large (ie. more than a second) in our tests.
//     *
//     * @return All the records consumed by the consumer within the specified duration.
//     */
//    def consumeRecordsFor[K, V](consumer: KafkaConsumer[K, V], duration: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
//        val startTime = System.currentTimeMillis()
//        val records = new ArrayBuffer[ConsumerRecord[K, V]]()
//        waitUntilTrue(() => {
//                records ++= consumer.poll(Duration.ofMillis(50)).asScala
//                System.currentTimeMillis() - startTime > duration
//        }, s"The timeout $duration was greater than the maximum wait time.")
//        records
//    }
//
//    def createTransactionalProducer[B <: KafkaBroker](
//    transactionalId: String,
//    brokers: Seq[B],
//    batchSize: Int = 16384,
//    transactionTimeoutMs: Long = 60000,
//    maxBlockMs: Long = 60000,
//    deliveryTimeoutMs: Int = 120000,
//    requestTimeoutMs: Int = 30000,
//    maxInFlight: Int = 5): KafkaProducer[Array[Byte], Array[Byte]] = {
//        val props = new Properties()
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
//        props.put(ProducerConfig.ACKS_CONFIG, "all")
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
//        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
//        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs.toString)
//        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs.toString)
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString)
//        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight.toString)
//        new KafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer, new ByteArraySerializer)
//    }
//
//    // Seeds the given topic with records with keys and values in the range [0..numRecords)
//    def seedTopicWithNumberedRecords[B <: KafkaBroker](
//    topic: String,
//    numRecords: Int,
//    brokers: Seq[B]): Unit = {
//        val props = new Properties()
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
//        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer, new ByteArraySerializer)
//        try {
//            for (i <- 0 until numRecords) {
//                producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, asBytes(i.toString), asBytes(i.toString)))
//            }
//            producer.flush()
//        } finally {
//            producer.close()
//        }
//    }
//
//    private def asString(bytes: Array[Byte]) = new String(bytes, StandardCharsets.UTF_8)
//
//    private def asBytes(string: String) = string.getBytes(StandardCharsets.UTF_8)
//
//    // Verifies that the record was intended to be committed by checking the headers for an expected transaction status
//    // If true, this will return the value as a string. It is expected that the record in question should have been created
//    // by the `producerRecordWithExpectedTransactionStatus` method.
//    def assertCommittedAndGetValue(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
//        record.headers.headers(transactionStatusKey).asScala.headOption match {
//            case Some(header) =>
//                assertEquals(asString(committedValue), asString(header.value), s"Got ${asString(header.value)} but expected the value to indicate " +
//                        s"committed status.")
//            case None =>
//                fail("expected the record header to include an expected transaction status, but received nothing.")
//        }
//        recordValueAsString(record)
//    }
//
//    def recordValueAsString(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
//        asString(record.value)
//    }
//
//    def producerRecordWithExpectedTransactionStatus(topic: String, partition: Integer, key: Array[Byte], value: Array[Byte], willBeCommitted: Boolean): ProducerRecord[Array[Byte], Array[Byte]] = {
//        val header = new Header {override def key() = transactionStatusKey
//            override def value() = if (willBeCommitted)
//                committedValue
//            else
//                abortedValue
//        }
//        new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, key, value, Collections.singleton(header))
//    }
//
//    def producerRecordWithExpectedTransactionStatus(topic: String, partition: Integer, key: String, value: String, willBeCommitted: Boolean): ProducerRecord[Array[Byte], Array[Byte]] = {
//        producerRecordWithExpectedTransactionStatus(topic, partition, asBytes(key), asBytes(value), willBeCommitted)
//    }
//
//    // Collect the current positions for all partition in the consumers current assignment.
//    def consumerPositions(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) : Map[TopicPartition, OffsetAndMetadata]  = {
//        val offsetsToCommit = new mutable.HashMap[TopicPartition, OffsetAndMetadata]()
//        consumer.assignment.forEach { topicPartition =>
//            offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
//        }
//        offsetsToCommit.toMap
//    }
//
//    def resetToCommittedPositions(consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {
//        val committed = consumer.committed(consumer.assignment).asScala.filter(_._2 != null).map { case (k, v) => k -> v.offset }
//
//        consumer.assignment.forEach { topicPartition =>
//            if (committed.contains(topicPartition))
//                consumer.seek(topicPartition, committed(topicPartition))
//            else
//                consumer.seekToBeginning(Collections.singletonList(topicPartition))
//        }
//    }
//
//    def incrementalAlterConfigs[B <: KafkaBroker](
//    servers: Seq[B],
//    adminClient: Admin,
//    props: Properties,
//    perBrokerConfig: Boolean,
//    opType: OpType = OpType.SET): AlterConfigsResult  = {
//        val configEntries = props.asScala.map { case (k, v) => new AlterConfigOp(new ConfigEntry(k, v), opType) }.toList.asJavaCollection
//        val configs = if (perBrokerConfig) {
//            servers.map { server =>
//                val resource = new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
//                (resource, configEntries)
//            }.toMap.asJava
//        } else {
//            Map(new ConfigResource(ConfigResource.Type.BROKER, "") -> configEntries).asJava
//        }
//        adminClient.incrementalAlterConfigs(configs)
//    }
//
//    def alterClientQuotas(adminClient: Admin, request: Map[ClientQuotaEntity, Map[String, Option[Double]]]): AlterClientQuotasResult = {
//        val entries = request.map { case (entity, alter) =>
//            val ops = alter.map { case (key, value) =>
//            new ClientQuotaAlteration.Op(key, value.map(Double.box).getOrElse(null))
//        }.asJavaCollection
//        new ClientQuotaAlteration(entity, ops)
//        }.asJavaCollection
//        adminClient.alterClientQuotas(entries)
//    }
//
//    def assertLeader(client: Admin, topicPartition: TopicPartition, expectedLeader: Int): Unit = {
//        waitForLeaderToBecome(client, topicPartition, Some(expectedLeader))
//    }
//
//    def assertNoLeader(client: Admin, topicPartition: TopicPartition): Unit = {
//        waitForLeaderToBecome(client, topicPartition, None)
//    }
//
//    def waitForOnlineBroker(client: Admin, brokerId: Int): Unit = {
//        waitUntilTrue(() => {
//                val nodes = client.describeCluster().nodes().get()
//                nodes.asScala.exists(_.id == brokerId)
//        }, s"Timed out waiting for brokerId $brokerId to come online")
//    }
//
//    def waitForLeaderToBecome(
//            client: Admin,
//            topicPartition: TopicPartition,
//            expectedLeaderOpt: Option[Int]
//    ): Unit = {
//        val topic = topicPartition.topic
//        val partitionId = topicPartition.partition
//
//        def currentLeader: Try[Option[Int]] = Try {
//            val topicDescription = client.describeTopics(List(topic).asJava).allTopicNames.get.get(topic)
//            topicDescription.partitions.asScala
//                    .find(_.partition == partitionId)
//                    .flatMap(partitionState => Option(partitionState.leader))
//        .map(_.id)
//        }
//
//        val (lastLeaderCheck, isLeaderElected) = computeUntilTrue(currentLeader) {
//            case Success(leaderOpt) => leaderOpt == expectedLeaderOpt
//            case Failure(e: ExecutionException) if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
//            case Failure(e) => throw e
//        }
//
//        assertTrue(isLeaderElected, s"Timed out waiting for leader to become $expectedLeaderOpt. " +
//                s"Last metadata lookup returned leader = ${lastLeaderCheck.getOrElse("unknown")}")
//    }
//
//    def waitForBrokersOutOfIsr(client: Admin, partition: Set[TopicPartition], brokerIds: Set[Int]): Unit = {
//        waitUntilTrue(
//                () => {
//                val description = client.describeTopics(partition.map(_.topic).asJava).allTopicNames.get.asScala
//                val isr = description
//                .values
//                .flatMap(_.partitions.asScala.flatMap(_.isr.asScala))
//                .map(_.id)
//                .toSet
//
//                brokerIds.intersect(isr).isEmpty
//        },
//                s"Expected brokers $brokerIds to no longer be in the ISR for $partition"
//    )
//    }
//
//    def waitForBrokersInIsr(client: Admin, partition: TopicPartition, brokerIds: Set[Int]): Unit = {
//        waitUntilTrue(
//                () => {
//                val description = client.describeTopics(Set(partition.topic).asJava).allTopicNames.get.asScala
//                val isr = description
//                .values
//                .flatMap(_.partitions.asScala.flatMap(_.isr.asScala))
//                .map(_.id)
//                .toSet
//
//                brokerIds.subsetOf(isr)
//        },
//                s"Expected brokers $brokerIds to be in the ISR for $partition"
//    )
//    }
//
//    def waitForReplicasAssigned(client: Admin, partition: TopicPartition, brokerIds: Seq[Int]): Unit = {
//        waitUntilTrue(
//                () => {
//                val description = client.describeTopics(Set(partition.topic).asJava).allTopicNames.get.asScala
//                val replicas = description
//                .values
//                .flatMap(_.partitions.asScala.flatMap(_.replicas.asScala))
//                .map(_.id)
//                .toSeq
//
//                brokerIds == replicas
//        },
//                s"Expected brokers $brokerIds to be the replicas for $partition"
//    )
//    }
//
//    /**
//     * Capture the console output during the execution of the provided function.
//     */
//    def grabConsoleOutput(f: => Unit) : String = {
//        val out = new ByteArrayOutputStream
//        try scala.Console.withOut(out)(f)
//    finally scala.Console.out.flush()
//        out.toString
//    }
//
//    /**
//     * Capture the console error during the execution of the provided function.
//     */
//    def grabConsoleError(f: => Unit) : String = {
//        val err = new ByteArrayOutputStream
//        try scala.Console.withErr(err)(f)
//    finally scala.Console.err.flush()
//        err.toString
//    }
//
//    /**
//     * Capture both the console output and console error during the execution of the provided function.
//     */
//    def grabConsoleOutputAndError(f: => Unit) : (String, String) = {
//        val out = new ByteArrayOutputStream
//        val err = new ByteArrayOutputStream
//        try scala.Console.withOut(out)(scala.Console.withErr(err)(f))
//    finally {
//            scala.Console.out.flush()
//            scala.Console.err.flush()
//        }
//        (out.toString, err.toString)
//    }
//
//    def assertFutureExceptionTypeEquals(future: KafkaFuture[_], clazz: Class[_ <: Throwable],
//                                        expectedErrorMessage: Option[String] = None): Unit = {
//        val cause = assertThrows(classOf[ExecutionException], () => future.get()).getCause
//        assertTrue(clazz.isInstance(cause), "Expected an exception of type " + clazz.getName + "; got type " +
//                cause.getClass.getName)
//        expectedErrorMessage.foreach(message => assertTrue(cause.getMessage.contains(message), s"Received error message : ${cause.getMessage}" +
//                s" does not contain expected error message : $message"))
//    }
//
//    def assertBadConfigContainingMessage(props: Properties, expectedExceptionContainsText: String): Unit = {
//        try {
//            KafkaConfig.fromProps(props)
//            fail("Expected illegal configuration but instead it was legal")
//        } catch {
//            case caught @ (_: ConfigException | _: IllegalArgumentException) =>
//                assertTrue(caught.getMessage.contains(expectedExceptionContainsText))
//        }
//    }
//
//    def totalMetricValue(broker: KafkaBroker, metricName: String): Long = {
//        totalMetricValue(broker.metrics, metricName)
//    }
//
//    def totalMetricValue(metrics: Metrics, metricName: String): Long = {
//        val allMetrics = metrics.metrics
//        val total = allMetrics.values().asScala.filter(_.metricName().name() == metricName)
//                .foldLeft(0.0)((total, metric) => total + metric.metricValue.asInstanceOf[Double])
//        total.toLong
//    }
//
//    def yammerGaugeValue[T](metricName: String): Option[T] = {
//        KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
//                .filter { case (k, _) => k.getMBeanName.endsWith(metricName) }
//      .values
//                .headOption
//                .map(_.asInstanceOf[Gauge[T]])
//                .map(_.value)
//    }
//
//    def meterCount(metricName: String): Long = {
//        KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
//                .filter { case (k, _) => k.getMBeanName.endsWith(metricName) }
//      .values
//                .headOption
//                .getOrElse(fail(s"Unable to find metric $metricName"))
//                .asInstanceOf[Meter]
//                .count
//    }

    public static void clearYammerMetrics() {
        for (MetricName metricName : KafkaYammerMetrics.defaultRegistry().allMetrics().keySet())
            KafkaYammerMetrics.defaultRegistry().removeMetric(metricName);
    }

//    def stringifyTopicPartitions(partitions: Set[TopicPartition]): String = {
//        Json.encodeAsString(Map("partitions" ->
//        partitions.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition).asJava).asJava).asJava)
//    }
//
//    def resource[R <: AutoCloseable, A](resource: R)(func: R => A): A = {
//        try {
//            func(resource)
//        } finally {
//            resource.close()
//        }
//    }
//
//    /**
//     * Set broker replication quotas and enable throttling for a set of partitions. This
//     * will override any previous replication quotas, but will leave the throttling status
//     * of other partitions unaffected.
//     */
//    def setReplicationThrottleForPartitions(admin: Admin,
//                                            brokerIds: Seq[Int],
//                                            partitions: Set[TopicPartition],
//                                            throttleBytes: Int): Unit = {
//        throttleAllBrokersReplication(admin, brokerIds, throttleBytes)
//        assignThrottledPartitionReplicas(admin, partitions.map(_ -> brokerIds).toMap)
//    }
//
//    /**
//     * Remove a set of throttled partitions and reset the overall replication quota.
//     */
//    def removeReplicationThrottleForPartitions(admin: Admin,
//                                               brokerIds: Seq[Int],
//                                               partitions: Set[TopicPartition]): Unit = {
//        removePartitionReplicaThrottles(admin, partitions)
//        resetBrokersThrottle(admin, brokerIds)
//    }
//
//    /**
//     * Throttles all replication across the cluster.
//     * @param adminClient is the adminClient to use for making connection with the cluster
//     * @param brokerIds all broker ids in the cluster
//     * @param throttleBytes is the target throttle
//     */
//    def throttleAllBrokersReplication(adminClient: Admin, brokerIds: Seq[Int], throttleBytes: Int): Unit = {
//        val throttleConfigs = Seq(
//                new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, throttleBytes.toString), AlterConfigOp.OpType.SET),
//                new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, throttleBytes.toString), AlterConfigOp.OpType.SET)
//        ).asJavaCollection
//
//        adminClient.incrementalAlterConfigs(
//                brokerIds.map { brokerId =>
//            new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString) -> throttleConfigs
//        }.toMap.asJava
//    ).all().get()
//    }
//
//    def resetBrokersThrottle(adminClient: Admin, brokerIds: Seq[Int]): Unit =
//    throttleAllBrokersReplication(adminClient, brokerIds, Int.MaxValue)
//
//    def assignThrottledPartitionReplicas(adminClient: Admin, allReplicasByPartition: Map[TopicPartition, Seq[Int]]): Unit = {
//        val throttles = allReplicasByPartition.groupBy(_._1.topic()).map {
//            case (topic, replicasByPartition) =>
//                new ConfigResource(TOPIC, topic) -> Seq(
//                    new AlterConfigOp(new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp, formatReplicaThrottles(replicasByPartition)), AlterConfigOp.OpType.SET),
//                    new AlterConfigOp(new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp, formatReplicaThrottles(replicasByPartition)), AlterConfigOp.OpType.SET)
//            ).asJavaCollection
//        }
//        adminClient.incrementalAlterConfigs(throttles.asJava).all().get()
//    }
//
//    def removePartitionReplicaThrottles(adminClient: Admin, partitions: Set[TopicPartition]): Unit = {
//        val throttles = partitions.map {
//            tp =>
//            new ConfigResource(TOPIC, tp.topic()) -> Seq(
//                    new AlterConfigOp(new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp, ""), AlterConfigOp.OpType.DELETE),
//                    new AlterConfigOp(new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp, ""), AlterConfigOp.OpType.DELETE)
//            ).asJavaCollection
//        }.toMap
//        adminClient.incrementalAlterConfigs(throttles.asJava).all().get()
//    }
//
//    def formatReplicaThrottles(moves: Map[TopicPartition, Seq[Int]]): String =
//            moves.flatMap { case (tp, assignment) =>
//        assignment.map(replicaId => s"${tp.partition}:$replicaId")
//    }.mkString(",")
//
//    def waitForAllReassignmentsToComplete(adminClient: Admin, pause: Long = 100L): Unit = {
//        waitUntilTrue(() => adminClient.listPartitionReassignments().reassignments().get().isEmpty,
//                s"There still are ongoing reassignments", pause = pause)
//    }
//
//    /**
//     * Find an Authorizer that we can call createAcls or deleteAcls on.
//     */
//    def pickAuthorizerForWrite[B <: KafkaBroker](
//    brokers: Seq[B],
//    controllers: Seq[ControllerServer],
//            ): JAuthorizer = {
//        if (controllers.isEmpty) {
//            brokers.head.authorizer.get
//        } else {
//            var result: JAuthorizer = null
//            TestUtils.retry(120000) {
//                val active = controllers.filter(_.controller.isActive).head
//                result = active.authorizer.get
//            }
//            result
//        }
//    }
//
//    val anonymousAuthorizableContext = new AuthorizableRequestContext() {
//        override def listenerName(): String = ""
//        override def securityProtocol(): SecurityProtocol = SecurityProtocol.PLAINTEXT
//        override def principal(): KafkaPrincipal = KafkaPrincipal.ANONYMOUS
//        override def clientAddress(): InetAddress = null
//        override def requestType(): Int = 0
//        override def requestVersion(): Int = 0
//        override def clientId(): String = ""
//        override def correlationId(): Int = 0
//    }
//
//    def addAndVerifyAcls[B <: KafkaBroker](
//    brokers: Seq[B],
//    acls: Set[AccessControlEntry],
//    resource: ResourcePattern,
//    controllers: Seq[ControllerServer] = Seq(),
//  ): Unit = {
//        val authorizerForWrite = pickAuthorizerForWrite(brokers, controllers)
//        val aclBindings = acls.map { acl => new AclBinding(resource, acl) }
//        authorizerForWrite.createAcls(anonymousAuthorizableContext, aclBindings.toList.asJava).asScala
//                .map(_.toCompletableFuture.get)
//                .foreach { result =>
//            result.exception.ifPresent { e => throw e }
//        }
//        val aclFilter = new AclBindingFilter(resource.toFilter, AccessControlEntryFilter.ANY)
//        (brokers.map(_.authorizer.get) ++ controllers.map(_.authorizer.get)).foreach {
//            authorizer => waitAndVerifyAcls(
//                    authorizer.acls(aclFilter).asScala.map(_.entry).toSet ++ acls,
//                    authorizer, resource)
//        }
//    }
//
//    def removeAndVerifyAcls[B <: KafkaBroker](
//    brokers: Seq[B],
//    acls: Set[AccessControlEntry],
//    resource: ResourcePattern,
//    controllers: Seq[ControllerServer] = Seq(),
//  ): Unit = {
//        val authorizerForWrite = pickAuthorizerForWrite(brokers, controllers)
//        val aclBindingFilters = acls.map { acl => new AclBindingFilter(resource.toFilter, acl.toFilter) }
//        authorizerForWrite.deleteAcls(anonymousAuthorizableContext, aclBindingFilters.toList.asJava).asScala
//                .map(_.toCompletableFuture.get)
//                .foreach { result =>
//            result.exception.ifPresent { e => throw e }
//        }
//        val aclFilter = new AclBindingFilter(resource.toFilter, AccessControlEntryFilter.ANY)
//        (brokers.map(_.authorizer.get) ++ controllers.map(_.authorizer.get)).foreach {
//            authorizer => waitAndVerifyAcls(
//                    authorizer.acls(aclFilter).asScala.map(_.entry).toSet -- acls,
//                    authorizer, resource)
//        }
//    }
//
//    def buildEnvelopeRequest(
//            request: AbstractRequest,
//            principalSerde: KafkaPrincipalSerde,
//            requestChannelMetrics: RequestChannel.Metrics,
//            startTimeNanos: Long,
//            dequeueTimeNanos: Long = -1,
//            fromPrivilegedListener: Boolean = true
//    ): RequestChannel.Request = {
//        val clientId = "id"
//        val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
//
//        val requestHeader = new RequestHeader(request.apiKey, request.version, clientId, 0)
//        val requestBuffer = request.serializeWithHeader(requestHeader)
//
//        val envelopeHeader = new RequestHeader(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion(), clientId, 0)
//        val envelopeBuffer = new EnvelopeRequest.Builder(
//                requestBuffer,
//                principalSerde.serialize(KafkaPrincipal.ANONYMOUS),
//                InetAddress.getLocalHost.getAddress
//        ).build().serializeWithHeader(envelopeHeader)
//
//        RequestHeader.parse(envelopeBuffer)
//
//        val envelopeContext = new RequestContext(envelopeHeader, "1", InetAddress.getLocalHost,
//                KafkaPrincipal.ANONYMOUS, listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY,
//                fromPrivilegedListener, Optional.of(principalSerde))
//
//        val envelopRequest = new RequestChannel.Request(
//                processor = 1,
//                context = envelopeContext,
//                startTimeNanos = startTimeNanos,
//                memoryPool = MemoryPool.NONE,
//                buffer = envelopeBuffer,
//                metrics = requestChannelMetrics,
//                envelope = None
//        )
//        envelopRequest.requestDequeueTimeNanos = dequeueTimeNanos
//        envelopRequest
//    }
//
//    def verifyNoUnexpectedThreads(context: String): Unit = {
//        // Threads which may cause transient failures in subsequent tests if not shutdown.
//        // These include threads which make connections to brokers and may cause issues
//        // when broker ports are reused (e.g. auto-create topics) as well as threads
//        // which reset static JAAS configuration.
//        val unexpectedThreadNames = Set(
//                ControllerEventManager.ControllerEventThreadName,
//                KafkaProducer.NETWORK_THREAD_PREFIX,
//                AdminClientUnitTestEnv.kafkaAdminClientNetworkThreadPrefix(),
//                AbstractCoordinator.HEARTBEAT_THREAD_PREFIX,
//                QuorumTestHarness.ZkClientEventThreadSuffix,
//                QuorumController.CONTROLLER_THREAD_SUFFIX
//        )
//
//        def unexpectedThreads: Set[String] = {
//                val allThreads = Thread.getAllStackTraces.keySet.asScala.map(thread => thread.getName)
//        allThreads.filter(t => unexpectedThreadNames.exists(s => t.contains(s))).toSet
//    }
//
//        val (unexpected, _) = TestUtils.computeUntilTrue(unexpectedThreads)(_.isEmpty)
//                assertTrue(unexpected.isEmpty,
//                        s"Found ${unexpected.size} unexpected threads during $context: " +
//                                s"${unexpected.mkString("`", ",", "`")}")
//    }
//
//    class TestControllerRequestCompletionHandler(expectedResponse: Option[AbstractResponse] = None)
//            extends ControllerRequestCompletionHandler {
//        var actualResponse: Option[ClientResponse] = Option.empty
//        val completed: AtomicBoolean = new AtomicBoolean(false)
//        val timedOut: AtomicBoolean = new AtomicBoolean(false)
//
//        override def onComplete(response: ClientResponse): Unit = {
//            actualResponse = Some(response)
//            expectedResponse.foreach { expected =>
//                assertEquals(expected, response.responseBody())
//            }
//            completed.set(true)
//        }
//
//        override def onTimeout(): Unit = {
//            timedOut.set(true)
//        }
//    }
}
