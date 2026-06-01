package broker.protocol;

/**
 * 业务消息体，Publisher 与 Subscriber 之间传递的数据单元。
 * <p>
 * 字段约定见 docs/项目分工与实现计划.md：
 * </p>
 * <ul>
 *   <li>{@code topic} — 主题，如 {@code network.alert}</li>
 *   <li>{@code payload} — JSON 或纯文本业务内容</li>
 *   <li>{@code timestamp} — 发送时间，{@code System.currentTimeMillis()}</li>
 * </ul>
 */
public class Message {

    private String topic;
    private String payload;
    private long timestamp;

    public Message() {
        // 无参构造，便于 JSON 反序列化
    }

    public Message(String topic, String payload) {
        this.topic = topic;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String topic, String payload, long timestamp) {
        this.topic = topic;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Message{topic='" + topic + "', payload='" + payload + "', timestamp=" + timestamp + '}';
    }
}
