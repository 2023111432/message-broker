package broker;

import broker.protocol.Message;

import java.net.Socket;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 单个客户端连接的状态封装。
 * <p>
 * 维护该连接已订阅的 topic 列表、最后心跳时间，并提供线程安全的写通道（向客户端推送 {@code PUSH}）。
 * </p>
 */
public class ClientSession {

    /** 客户端唯一标识，由 REGISTER 命令设置 */
    private volatile String clientId;

    private final Socket socket;

    /** 本 Session 已订阅的主题集合 */
    private final Set<String> subscribedTopics;

    /** 最后一次收到 HEARTBEAT 的时间戳（毫秒） */
    private volatile long lastHeartbeatMillis;

    public ClientSession(Socket socket) {
        this.socket = socket;
        this.subscribedTopics = ConcurrentHashMap.newKeySet();
        this.lastHeartbeatMillis = System.currentTimeMillis();
    }

    /**
     * 绑定客户端 ID（REGISTER 成功后调用）。
     */
    public void setClientId(String clientId) {
        // TODO: 校验非空、是否重复注册等
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public Socket getSocket() {
        return socket;
    }

    /**
     * 记录本 Session 订阅了一个 topic。
     */
    public void addSubscription(String topic) {
        // TODO: 校验 topic 非空
        subscribedTopics.add(topic);
    }

    /**
     * 取消本 Session 对某 topic 的订阅。
     */
    public void removeSubscription(String topic) {
        subscribedTopics.remove(topic);
    }

    /**
     * @return 不可变的已订阅 topic 快照
     */
    public Set<String> getSubscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }

    /**
     * 更新最后心跳时间（收到 HEARTBEAT 时调用）。
     */
    public void touchHeartbeat() {
        lastHeartbeatMillis = System.currentTimeMillis();
    }

    public long getLastHeartbeatMillis() {
        return lastHeartbeatMillis;
    }

    /**
     * 向客户端推送一条消息（封装为 PUSH 命令帧）。
     * <p>写 Socket 时需对本 Session 加锁，避免并发写导致帧乱序。</p>
     *
     * @param message 待推送的业务消息
     */
    public void push(Message message) {
        // TODO: 使用 ProtocolCodec 编码 PUSH 命令，写入 socket.getOutputStream()
    }

    /**
     * 关闭连接并释放资源。
     */
    public void close() {
        // TODO: 关闭 socket 输入/输出流及 socket 本身
    }

    /**
     * @return 连接是否仍然可用
     */
    public boolean isOpen() {
        // TODO: 根据 socket 状态判断
        return socket != null && !socket.isClosed();
    }
}
