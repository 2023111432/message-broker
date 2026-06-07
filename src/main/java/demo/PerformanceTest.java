package demo;

import client.PublisherClient;
import client.SubscriberClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

/**
 * 简易性能测试：吞吐、延迟、多订阅者扇出、多生产者并发。
 * <p>
 * 运行前启动 Broker：{@code mvn compile exec:java@broker}
 * 本类：{@code mvn compile exec:java@perf}
 * 可选参数：{@code [吞吐条数] [延迟采样数] [扇出消息数]}，默认 10000 / 1000 / 100
 * </p>
 */
public class PerformanceTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9090;   // 统一为同伴服务端默认端口

    private static final Pattern SEND_TIME =
            Pattern.compile("\"sendTime\"\\s*:\\s*(\\d+)");

    public static void main(String[] args) throws Exception {
        int throughputCount = arg(args, 0, 10_000);
        int latencySamples = arg(args, 1, 1_000);
        int fanoutMessages = arg(args, 2, 100);

        System.out.println("========== 消息中间件性能测试 ==========");
        System.out.printf("Broker %s:%d | 吞吐 N=%d | 延迟采样=%d | 扇出 M=%d%n%n",
                HOST, PORT, throughputCount, latencySamples, fanoutMessages);

        boolean fanoutOk = runFanOutTest(fanoutMessages);
        System.out.println();
        runMultiProducerTest();           // 新增：多生产者并发测试
        System.out.println();
        runLatencyTest(latencySamples);
        System.out.println();
        runThroughputTest(throughputCount);

        System.out.println();
        System.out.println("========== 测试结束 ==========");
        if (!fanoutOk) {
            System.exit(1);
        }
    }

    /** 3 个订阅者同主题，验证每条消息均被各自收到 */
    static boolean runFanOutTest(int messageCount) throws Exception {
        System.out.println("--- 1. 多订阅者扇出（并发投递）---");
        String topic = "perf.fanout";
        int subscribers = 3;
        AtomicInteger[] counts = new AtomicInteger[subscribers];
        SubscriberClient[] clients = new SubscriberClient[subscribers];

        for (int i = 0; i < subscribers; i++) {
            counts[i] = new AtomicInteger();
            clients[i] = new SubscriberClient();
            clients[i].setClientId("perf-sub-" + i);
            clients[i].connect(HOST, PORT);
            int idx = i;
            clients[i].subscribe(topic, (t, payload) -> counts[idx].incrementAndGet());
        }
        Thread.sleep(400);

        PublisherClient publisher = new PublisherClient();
        publisher.setClientId("perf-pub-fanout");
        publisher.connect(HOST, PORT);
        for (int i = 0; i < messageCount; i++) {
            publisher.publish(topic, "{\"seq\":" + i + "}");
        }
        Thread.sleep(1500);

        boolean ok = true;
        for (int i = 0; i < subscribers; i++) {
            int got = counts[i].get();
            System.out.printf("  订阅者 perf-sub-%d 收到 %d / %d 条%n", i, got, messageCount);
            if (got != messageCount) {
                ok = false;
            }
        }
        System.out.println(ok ? "  结果: 通过（每条消息均送达全部订阅者）" : "  结果: 失败（存在丢消息或未投递）");

        publisher.disconnect();
        for (SubscriberClient c : clients) {
            c.disconnect();
        }
        return ok;
    }

    /** 多生产者并发测试（对比不同生产者数量的吞吐率） */
    static void runMultiProducerTest() throws Exception {
        System.out.println("--- 2. 多生产者并发吞吐对比 ---");
        String topic = "perf.multiprod";
        int consumers = 10;
        int messagesPerProducer = 2000;   // 每个生产者发送 2000 条

        // 配置1：1个生产者
        System.out.println(">>> 配置1: 1 个生产者, " + consumers + " 个消费者 <<<");
        runMultiProducerScenario(1, consumers, messagesPerProducer, topic);

        // 配置2：4个生产者
        System.out.println(">>> 配置2: 4 个生产者, " + consumers + " 个消费者 <<<");
        runMultiProducerScenario(4, consumers, messagesPerProducer, topic);
    }

    /** 执行指定生产者数和消费者数的测试 */
    static void runMultiProducerScenario(int producerCount, int consumerCount,
                                         int messagesPerProd, String topic) throws Exception {
        // 启动所有消费者
        SubscriberClient[] consumers = new SubscriberClient[consumerCount];
        AtomicInteger[] receivedCounts = new AtomicInteger[consumerCount];
        CountDownLatch[] readyLatches = new CountDownLatch[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            receivedCounts[i] = new AtomicInteger();
            readyLatches[i] = new CountDownLatch(1);
            consumers[i] = new SubscriberClient();
            consumers[i].setClientId("mp-consumer-" + i);
            consumers[i].connect(HOST, PORT);
            final int idx = i;
            consumers[i].subscribe(topic, (t, payload) -> {
                receivedCounts[idx].incrementAndGet();
            });
        }
        // 等待所有消费者订阅注册完成（更可靠的方式）
        Thread.sleep(500);

        // 启动多个生产者线程并发发送
        ExecutorService executor = Executors.newFixedThreadPool(producerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(producerCount);

        long start = System.nanoTime();
        for (int p = 0; p < producerCount; p++) {
            final int producerId = p;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    PublisherClient pub = new PublisherClient();
                    pub.setClientId("mp-producer-" + producerId);
                    pub.connect(HOST, PORT);
                    for (int i = 0; i < messagesPerProd; i++) {
                        pub.publish(topic, "{\"prod\":" + producerId + ",\"seq\":" + i + "}");
                    }
                    pub.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        startLatch.countDown();
        doneLatch.await();  // 等待所有生产者发送完成
        long publishEnd = System.nanoTime();

        // 等待消费者接收完成（根据消息量动态等待，最多 60 秒）
        int totalExpected = producerCount * messagesPerProd;
        long deadline = System.currentTimeMillis() + 60_000;
        boolean allReceived = false;
        while (!allReceived && System.currentTimeMillis() < deadline) {
            allReceived = true;
            for (int i = 0; i < consumerCount; i++) {
                if (receivedCounts[i].get() < totalExpected) {
                    allReceived = false;
                    break;
                }
            }
            if (!allReceived) Thread.sleep(100);
        }
        long finalEnd = System.nanoTime();

        executor.shutdown();

        // 输出每个消费者的接收情况
        boolean ok = true;
        for (int i = 0; i < consumerCount; i++) {
            int received = receivedCounts[i].get();
            if (received != totalExpected) {
                System.out.printf("  消费者 %d 收到 %d / %d 条%n", i, received, totalExpected);
                ok = false;
            } else {
                System.out.printf("  消费者 %d 收到 %d 条 ✓%n", i, received);
            }
        }

        int totalReceived = Arrays.stream(receivedCounts).mapToInt(AtomicInteger::get).sum();
        double publishSec = (publishEnd - start) / 1_000_000_000.0;
        double e2eSec = (finalEnd - start) / 1_000_000_000.0;
        double publishTps = (producerCount * messagesPerProd) / publishSec;
        double e2eTps = (producerCount * messagesPerProd) / e2eSec;

        System.out.printf("  总发送: %d 条, 总接收: %d 条 (扇出系数 %d)%n",
                totalExpected, totalReceived, consumerCount);
        System.out.printf("  发布耗时: %.3f s (发布侧吞吐 %.0f 条/秒)%n", publishSec, publishTps);
        System.out.printf("  端到端耗时: %.3f s (端到端吞吐 %.0f 条/秒)%n", e2eSec, e2eTps);
        System.out.println(ok ? "  结果: 通过（所有消费者收到全部消息）" : "  结果: 失败（部分消费者未收齐）");

        // 关闭消费者
        for (SubscriberClient c : consumers) {
            c.disconnect();
        }
    }

    /** payload 携带 sendTime，统计端到端延迟平均与 P95 */
    static void runLatencyTest(int samples) throws Exception {
        System.out.println("--- 3. 端到端延迟 ---");
        String topic = "perf.latency";
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>(samples));

        SubscriberClient subscriber = new SubscriberClient();
        subscriber.setClientId("perf-sub-latency");
        subscriber.connect(HOST, PORT);
        subscriber.subscribe(topic, (t, payload) -> {
            long sendTime = parseSendTime(payload);
            if (sendTime > 0) {
                latencies.add(System.currentTimeMillis() - sendTime);
            }
        });
        Thread.sleep(300);

        PublisherClient publisher = new PublisherClient();
        publisher.setClientId("perf-pub-latency");
        publisher.connect(HOST, PORT);
        for (int i = 0; i < samples; i++) {
            long sendTime = System.currentTimeMillis();
            publisher.publish(topic, "{\"sendTime\":" + sendTime + "}");
        }

        long deadline = System.currentTimeMillis() + 30_000;
        while (latencies.size() < samples && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        Thread.sleep(200);

        publisher.disconnect();
        subscriber.disconnect();

        if (latencies.isEmpty()) {
            System.out.println("  结果: 失败（未收到延迟样本）");
            return;
        }

        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        long p95 = sorted.get((int) Math.min(sorted.size() - 1, Math.ceil(sorted.size() * 0.95) - 1));
        long min = sorted.get(0);
        long max = sorted.get(sorted.size() - 1);

        System.out.printf("  采样数: %d / %d%n", sorted.size(), samples);
        System.out.printf("  平均延迟: %.2f ms%n", avg);
        System.out.printf("  P95 延迟: %d ms%n", p95);
        System.out.printf("  最小 / 最大: %d / %d ms%n", min, max);
    }

    /** 单发布者连续 N 条，统计端到端吞吐（条/秒） */
    static void runThroughputTest(int messageCount) throws Exception {
        System.out.println("--- 4. 端到端吞吐 ---");
        String topic = "perf.throughput";
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger received = new AtomicInteger();

        SubscriberClient subscriber = new SubscriberClient();
        subscriber.setClientId("perf-sub-tput");
        subscriber.connect(HOST, PORT);
        subscriber.subscribe(topic, (t, payload) -> {
            received.incrementAndGet();
            latch.countDown();
        });
        Thread.sleep(300);

        PublisherClient publisher = new PublisherClient();
        publisher.setClientId("perf-pub-tput");
        publisher.connect(HOST, PORT);

        long start = System.nanoTime();
        for (int i = 0; i < messageCount; i++) {
            publisher.publish(topic, "{\"i\":" + i + "}");
        }
        long publishDone = System.nanoTime();

        boolean allReceived = latch.await(120, TimeUnit.SECONDS);
        long end = System.nanoTime();

        publisher.disconnect();
        subscriber.disconnect();

        double publishSec = (publishDone - start) / 1_000_000_000.0;
        double e2eSec = (end - start) / 1_000_000_000.0;
        double publishTps = messageCount / publishSec;
        double e2eTps = messageCount / e2eSec;

        System.out.printf("  发布 %d 条耗时: %.3f s（仅发布侧 %.0f 条/秒）%n",
                messageCount, publishSec, publishTps);
        System.out.printf("  订阅收到 %d 条，端到端耗时: %.3f s%n", received.get(), e2eSec);
        System.out.printf("  端到端吞吐: %.0f 条/秒%n", e2eTps);
        System.out.println(allReceived && received.get() == messageCount
                ? "  结果: 通过（无丢消息）"
                : "  结果: 失败或超时（收到 " + received.get() + " 条）");
    }

    private static int arg(String[] args, int index, int defaultValue) {
        if (args.length > index) {
            return Integer.parseInt(args[index]);
        }
        return defaultValue;
    }

    private static long parseSendTime(String payload) {
        Matcher m = SEND_TIME.matcher(payload);
        return m.find() ? Long.parseLong(m.group(1)) : -1;
    }
}