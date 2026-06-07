package demo;

import client.PublisherClient;
import client.SubscriberClient;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 进销存系统演示（发布/订阅）。
 * <p>
 * 订单服务发布 {@code order.created}，库存服务扣减库存，通知服务发送短信/邮件（均为日志模拟）。
 * </p>
 * <p>
 * 运行前请先启动 Broker：
 * 终端1：{@code mvn compile exec:java@broker}
 * 终端2：{@code mvn compile exec:java@pssdemo}
 * </p>
 */
public class PssDemo {

    private static final String BROKER_HOST = "localhost";
    private static final int BROKER_PORT = 9090;
    private static final String TOPIC = "order.created";

    private static final Pattern NUMBER_FIELD =
            Pattern.compile("\"quantity\"\\s*:\\s*(\\d+)");

    public static void main(String[] args) throws Exception {
        System.out.println("========== 进销存演示开始 ==========");
        System.out.println("Broker: " + BROKER_HOST + ":" + BROKER_PORT + "  主题: " + TOPIC);
        System.out.println();

        SubscriberClient inventoryService = new SubscriberClient();
        inventoryService.setClientId("inventory-service");
        inventoryService.connect(BROKER_HOST, BROKER_PORT);
        inventoryService.subscribe(TOPIC, (topic, payload) -> {
            String orderId = extractField(payload, "orderId");
            String productId = extractField(payload, "productId");
            int quantity = extractQuantity(payload);
            System.out.println("[库存服务] 收到订单消息: " + payload);
            System.out.println("[库存服务] 订单 " + orderId + "：商品 " + productId
                    + " 库存减 " + quantity);
        });
        System.out.println("[库存服务] 已连接并订阅 " + TOPIC);

        SubscriberClient notifyService = new SubscriberClient();
        notifyService.setClientId("notify-service");
        notifyService.connect(BROKER_HOST, BROKER_PORT);
        notifyService.subscribe(TOPIC, (topic, payload) -> {
            String orderId = extractField(payload, "orderId");
            String userId = extractField(payload, "userId");
            System.out.println("[通知服务] 收到订单消息: " + payload);
            System.out.println("[通知服务] 已向用户 " + userId + " 发送订单 " + orderId + " 的短信/邮件通知");
        });
        System.out.println("[通知服务] 已连接并订阅 " + TOPIC);

        Thread.sleep(500);

        PublisherClient orderService = new PublisherClient();
        orderService.setClientId("order-service");
        orderService.connect(BROKER_HOST, BROKER_PORT);
        System.out.println("[订单服务] 已连接，准备创建订单\n");

        String[] orders = {
                "{\"orderId\":\"ORD1001\",\"productId\":\"P100\",\"quantity\":2,\"userId\":\"U001\"}",
                "{\"orderId\":\"ORD1002\",\"productId\":\"P200\",\"quantity\":1,\"userId\":\"U002\"}",
                "{\"orderId\":\"ORD1003\",\"productId\":\"P300\",\"quantity\":5,\"userId\":\"U003\"}"
        };

        for (int i = 0; i < orders.length; i++) {
            String orderJson = orders[i];
            System.out.println("--- 订单 " + (i + 1) + " ---");
            System.out.println("[订单服务] 创建新订单: " + orderJson);
            orderService.publish(TOPIC, orderJson);
            System.out.println("[订单服务] 消息已发布到主题 " + TOPIC + "，等待消费者异步处理…\n");
            Thread.sleep(800);
        }

        Thread.sleep(1500);

        inventoryService.disconnect();
        notifyService.disconnect();
        orderService.disconnect();

        System.out.println("========== 进销存演示结束，所有客户端已断开 ==========");
    }

    /** 提取 JSON 字符串中的引号字段，如 orderId、productId */
    static String extractField(String json, String fieldName) {
        Matcher m = Pattern.compile("\"" + Pattern.quote(fieldName) + "\"\\s*:\\s*\"([^\"]*)\"")
                .matcher(json);
        return m.find() ? m.group(1) : "unknown";
    }

    /** 提取 quantity 数字字段 */
    static int extractQuantity(String json) {
        Matcher m = NUMBER_FIELD.matcher(json);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return 1;
    }
}
