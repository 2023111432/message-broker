package broker.protocol;

import java.io.InputStream;
import java.io.OutputStream;
import protocol.Command;

/**
 * 协议编解码器：负责 Socket 字节流与 {@link Command} / {@link Message} 之间的转换。
 * <p>
 * 粘包处理（二选一，选简单的即可）：
 * </p>
 * <ul>
 *   <li>每行一个 JSON（{@code \n} 分隔）— 实现最快</li>
 *   <li>4 字节长度 + body — 稍规范</li>
 * </ul>
 */
public final class ProtocolCodec {

    private ProtocolCodec() {
        // 工具类，禁止实例化
    }

    /**
     * 从输入流读取一帧命令（阻塞直到完整帧到达或连接关闭）。
     *
     * @param in 客户端 Socket 输入流
     * @return 解析后的协议帧对象；连接关闭时可返回 {@code null}
     */
    public static ProtocolFrame readFrame(InputStream in) {
        // TODO: 按选定粘包方案读取并反序列化为 ProtocolFrame
        throw new UnsupportedOperationException("TODO: implement readFrame");
    }

    /**
     * 将协议帧写入输出流。
     *
     * @param out   输出流
     * @param frame 待发送的帧
     */
    public static void writeFrame(OutputStream out, ProtocolFrame frame) {
        // TODO: 序列化 frame 并写入 out，必要时 flush
        throw new UnsupportedOperationException("TODO: implement writeFrame");
    }

    /**
     * 协议帧：命令类型 + 可选字段（clientId、topic、message 等）。
     * <p>具体 JSON 字段见 docs/协议说明.md（待编写）。</p>
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
