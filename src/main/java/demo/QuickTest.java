package demo;

import client.PublisherClient;
import client.SubscriberClient;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 简易联调测试：订阅 test 主题，发布一条消息，验证能否收到推送。
 * <p>
 * 运行前请先启动 Broker（端口 9090）：
 * 终端1：{@code mvn compile exec:java@broker}
 * 终端2：{@code mvn compile exec:java@quicktest}
 * </p>
 */
public class QuickTest {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9090;
        AtomicBoolean received = new AtomicBoolean(false);

        SubscriberClient subscriber = new SubscriberClient();
        subscriber.setClientId("quicktest-subscriber");
        subscriber.connect(host, port);
        subscriber.subscribe("test", (topic, payload) -> {
            System.out.println("Received: " + payload);
            received.set(true);
        });
        System.out.println("Subscribed to test");

        Thread.sleep(300);

        PublisherClient publisher = new PublisherClient();
        publisher.setClientId("quicktest-publisher");
        publisher.connect(host, port);
        String message = "Hello, Broker!";
        publisher.publish("test", message);
        System.out.println("Published message: " + message);

        Thread.sleep(2000);

        subscriber.disconnect();
        publisher.disconnect();

        if (received.get()) {
            System.out.println("Test passed");
        } else {
            System.out.println("Test failed");
        }
    }
}
