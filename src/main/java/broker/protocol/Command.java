package broker.protocol;

/**
 * 客户端与服务端之间的命令类型。
 * <p>
 * 帧格式与 JSON 字段见 <b>docs/协议说明.md</b>（v1.0，一行一 JSON + {@code \n} 结尾）。
 * </p>
 */
public enum Command {

    /** 客户端 → 服务端：注册 clientId */
    REGISTER,

    /** 客户端 → 服务端：订阅指定 topic */
    SUBSCRIBE,

    /** 客户端 → 服务端：取消订阅（可选增强） */
    UNSUBSCRIBE,

    /** 客户端 → 服务端：发布消息，Broker 将其入队等待分发 */
    PUBLISH,

    /** 服务端 → 客户端：推送消息给订阅者 */
    PUSH,

    /** 双向：心跳保活 */
    HEARTBEAT,

    /** 双向：确认收到（简单场景可省略） */
    ACK;

    /**
     * 从协议字符串解析命令类型（JSON 字段 {@code type} 使用）。
     *
     * @param value 命令名字符串，忽略大小写
     * @return 对应的 {@link Command}
     * @throws IllegalArgumentException 未知命令
     */
    public static Command fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("command type is blank");
        }
        return Command.valueOf(value.trim().toUpperCase());
    }
}
