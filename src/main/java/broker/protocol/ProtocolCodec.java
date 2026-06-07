package broker.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 协议编解码器：负责 Socket 字节流与 {@link Command} / {@link Message} 之间的转换。
 * <p>
 * <b>v1.0</b>：UTF-8，每帧一行 JSON，以 {@code \n} 结尾。详见 docs/协议说明.md。
 * </p>
 */
public final class ProtocolCodec {

    /** 帧分隔符（换行） */
    public static final byte FRAME_DELIMITER = '\n';

    /** 帧编码 */
    public static final String CHARSET_NAME = StandardCharsets.UTF_8.name();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ProtocolCodec() {
        // 工具类，禁止实例化
    }

    /**
     * 从输入流读取一帧命令（阻塞直到完整帧到达或连接关闭）。
     *
     * @param in 客户端 Socket 输入流
     * @return 解析后的协议帧；连接正常关闭（EOF）时返回 {@code null}
     * @throws IOException 读流失败或 JSON 无法解析
     */
    public static ProtocolFrame readFrame(InputStream in) throws IOException {
        String line = readLine(in);
        if (line == null) {
            return null;
        }
        // 协议约定：空行忽略，继续读下一帧
        while (line.isBlank()) {
            line = readLine(in);
            if (line == null) {
                return null;
            }
        }
        return parseFrame(line);
    }

    /**
     * 将协议帧序列化为 JSON 并写入输出流（末尾追加 {@code \n} 并 flush）。
     */
    public static void writeFrame(OutputStream out, ProtocolFrame frame) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", frame.getType().name());

        if (frame.getClientId() != null) {
            root.put("clientId", frame.getClientId());
        }
        if (frame.getTopic() != null) {
            root.put("topic", frame.getTopic());
        }
        if (frame.getMessage() != null) {
            Message message = frame.getMessage();
            ObjectNode messageNode = MAPPER.createObjectNode();
            messageNode.put("topic", message.getTopic());
            messageNode.put("payload", message.getPayload());
            messageNode.put("timestamp", message.getTimestamp());
            root.set("message", messageNode);
        }

        byte[] bytes = MAPPER.writeValueAsBytes(root);
        out.write(bytes);
        out.write(FRAME_DELIMITER);
        out.flush();
    }

    /** 按字节读取直到 {@code \n}，不含分隔符本身 */
    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == FRAME_DELIMITER) {
                break;
            }
            buffer.write(b);
        }
        // EOF 且没有任何数据 → 连接已关闭
        if (b == -1 && buffer.size() == 0) {
            return null;
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    /** 将一行 JSON 解析为 {@link ProtocolFrame} */
    private static ProtocolFrame parseFrame(String json) throws IOException {
        JsonNode root = MAPPER.readTree(json);

        JsonNode typeNode = root.get("type");
        if (typeNode == null || typeNode.asText().isBlank()) {
            throw new IOException("missing required field: type");
        }

        ProtocolFrame frame = new ProtocolFrame();
        frame.setType(Command.fromString(typeNode.asText()));

        if (root.hasNonNull("clientId")) {
            frame.setClientId(root.get("clientId").asText());
        }
        if (root.hasNonNull("topic")) {
            frame.setTopic(root.get("topic").asText());
        }
        if (root.has("message") && !root.get("message").isNull()) {
            JsonNode messageNode = root.get("message");
            Message message = new Message();
            message.setTopic(messageNode.path("topic").asText(null));
            message.setPayload(messageNode.path("payload").asText(null));
            message.setTimestamp(messageNode.path("timestamp").asLong(0L));
            frame.setMessage(message);
        }
        return frame;
    }

    /**
     * 协议帧：对应 docs/协议说明.md §3 中的一行 JSON。
     */
    public static class ProtocolFrame {

        private Command type;
        private String clientId;
        private String topic;
        private Message message;

        public Command getType() {
            return type;
        }

        public void setType(Command type) {
            this.type = type;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Message getMessage() {
            return message;
        }

        public void setMessage(Message message) {
            this.message = message;
        }
    }
}
