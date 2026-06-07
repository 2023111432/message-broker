package broker;

import broker.protocol.Command;
import broker.protocol.Message;
import broker.protocol.ProtocolCodec;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 单个客户端连接的状态封装。
 * <p>
 * 维护该连接已订阅的 topic 列表、最后心跳时间，并提供线程安全的写通道（向客户端推送 {@code PUSH}）。
 * </p>
 */
public class ClientSession {

    private static final Logger LOG = Logger.getLogger(ClientSession.class.getName());

    /** 客户端唯一标识，由 REGISTER 命令设置 */
    private volatile String clientId;

    private final Socket socket;

    /** 写 Socket 时加锁，防止 MessageDispatcher 并发 push 导致帧交叉 */
    private final Object writeLock = new Object();

    /** 本 Session 已订阅的主题集合 */
    private final Set<String> subscribedTopics;

    /** 最后一次收到任意有效帧的时间戳（毫秒） */
    private volatile long lastHeartbeatMillis;

    public ClientSession(Socket socket) {
        this.socket = socket;
        this.subscribedTopics = ConcurrentHashMap.newKeySet();
        this.lastHeartbeatMillis = System.currentTimeMillis();
    }

    /**
     * 绑定客户端 ID（REGISTER 成功后调用）。
     * 协议约定：若已注册则忽略第二次 REGISTER。
     */
    public void setClientId(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
        if (this.clientId != null) {
            LOG.info(() -> "ignore duplicate REGISTER from " + this.clientId);
            return;
        }
        this.clientId = clientId.trim();
    }

    public String getClientId() {
        return clientId;
    }

    /** 是否已完成 REGISTER */
    public boolean isRegistered() {
        return clientId != null;
    }

    public Socket getSocket() {
        return socket;
    }

    public void addSubscription(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        subscribedTopics.add(topic.trim());
    }

    public void removeSubscription(String topic) {
        if (topic != null) {
            subscribedTopics.remove(topic);
        }
    }

    public Set<String> getSubscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }

    /** 收到任意有效帧时更新（含 HEARTBEAT、PUBLISH 等） */
    public void touchHeartbeat() {
        lastHeartbeatMillis = System.currentTimeMillis();
    }

    public long getLastHeartbeatMillis() {
        return lastHeartbeatMillis;
    }

    /**
     * 向客户端推送一条消息（封装为 PUSH 命令帧）。
     */
    public void push(Message message) {
        synchronized (writeLock) {
            if (!isOpen()) {
                return;
            }
            try {
                ProtocolCodec.ProtocolFrame frame = new ProtocolCodec.ProtocolFrame();
                frame.setType(Command.PUSH);
                frame.setMessage(message);
                ProtocolCodec.writeFrame(socket.getOutputStream(), frame);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "push failed for client " + clientId, e);
                closeQuietly();
            }
        }
    }

    public void close() {
        synchronized (writeLock) {
            closeQuietly();
        }
    }

    private void closeQuietly() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "close socket", e);
        }
    }

    public boolean isOpen() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    @Override
    public String toString() {
        return "ClientSession{clientId='" + clientId + "', open=" + isOpen() + "}";
    }
}
