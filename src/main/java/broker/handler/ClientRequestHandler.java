package broker.handler;

import broker.ClientSession;
import broker.core.HeartbeatMonitor;
import broker.core.SubscriptionRegistry;
import broker.protocol.Command;
import broker.protocol.Message;
import broker.protocol.ProtocolCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 客户端请求处理器：每个连接在线程池中运行的 Runnable。
 * <p>
 * 循环从 Socket 读帧 → 解析 {@link ProtocolCodec.ProtocolFrame} → 根据 {@link Command} 分发处理。
 * </p>
 */
public class ClientRequestHandler implements Runnable {

    private static final Logger LOG = Logger.getLogger(ClientRequestHandler.class.getName());

    private final ClientSession session;
    private final SubscriptionRegistry subscriptionRegistry;
    private final LinkedBlockingQueue<Message> outboundQueue;
    private final HeartbeatMonitor heartbeatMonitor;

    /** 收到非法命令或未 REGISTER 就操作时置 true，跳出读循环 */
    private volatile boolean shouldClose;

    public ClientRequestHandler(ClientSession session,
                                SubscriptionRegistry subscriptionRegistry,
                                LinkedBlockingQueue<Message> outboundQueue,
                                HeartbeatMonitor heartbeatMonitor) {
        this.session = session;
        this.subscriptionRegistry = subscriptionRegistry;
        this.outboundQueue = outboundQueue;
        this.heartbeatMonitor = heartbeatMonitor;
    }

    @Override
    public void run() {
        LOG.info(() -> "connection accepted from " + session.getSocket().getRemoteSocketAddress());
        try {
            InputStream in = session.getSocket().getInputStream();
            while (session.isOpen() && !shouldClose) {
                ProtocolCodec.ProtocolFrame frame = ProtocolCodec.readFrame(in);
                if (frame == null) {
                    // 对端正常关闭连接
                    break;
                }
                // 任意有效帧均视为活跃（协议：90s 内须收到任意帧）
                session.touchHeartbeat();
                handleFrame(frame);
                if (shouldClose) {
                    break;
                }
            }
        } catch (IOException e) {
            if (session.isOpen()) {
                LOG.log(Level.WARNING, "IO error on session " + session.getClientId(), e);
            }
        } finally {
            cleanup();
        }
    }

    private void handleFrame(ProtocolCodec.ProtocolFrame frame) {
        if (frame.getType() == null) {
            reject("missing command type");
            return;
        }

        switch (frame.getType()) {
            case REGISTER -> handleRegister(frame);
            case SUBSCRIBE -> handleSubscribe(frame);
            case UNSUBSCRIBE -> handleUnsubscribe(frame);
            case PUBLISH -> handlePublish(frame);
            case HEARTBEAT -> handleHeartbeat(frame);
            case PUSH, ACK -> reject("client must not send " + frame.getType());
            default -> reject("unknown command: " + frame.getType());
        }
    }

    private void handleRegister(ProtocolCodec.ProtocolFrame frame) {
        String clientId = frame.getClientId();
        if (clientId == null || clientId.isBlank()) {
            reject("REGISTER requires clientId");
            return;
        }
        try {
            session.setClientId(clientId);
            LOG.info(() -> "registered clientId=" + session.getClientId());
        } catch (IllegalArgumentException e) {
            reject(e.getMessage());
        }
    }

    private void handleSubscribe(ProtocolCodec.ProtocolFrame frame) {
        if (!requireRegistered()) {
            return;
        }
        String topic = frame.getTopic();
        if (topic == null || topic.isBlank()) {
            reject("SUBSCRIBE requires topic");
            return;
        }
        subscriptionRegistry.subscribe(topic, session);
    }

    private void handleUnsubscribe(ProtocolCodec.ProtocolFrame frame) {
        if (!requireRegistered()) {
            return;
        }
        String topic = frame.getTopic();
        if (topic == null || topic.isBlank()) {
            reject("UNSUBSCRIBE requires topic");
            return;
        }
        subscriptionRegistry.unsubscribe(topic, session);
    }

    private void handlePublish(ProtocolCodec.ProtocolFrame frame) {
        if (!requireRegistered()) {
            return;
        }
        Message message = frame.getMessage();
        if (message == null || message.getTopic() == null || message.getTopic().isBlank()) {
            reject("PUBLISH requires message.topic");
            return;
        }
        if (message.getPayload() == null) {
            reject("PUBLISH requires message.payload");
            return;
        }
        if (message.getTimestamp() <= 0) {
            message.setTimestamp(System.currentTimeMillis());
        }
        try {
            // 异步入队，发布线程不等待分发完成
            outboundQueue.put(message);
            LOG.fine(() -> "enqueued publish from " + session.getClientId()
                    + ", topic=" + message.getTopic());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shouldClose = true;
        }
    }

    private void handleHeartbeat(ProtocolCodec.ProtocolFrame frame) {
        // touchHeartbeat 已在 run() 中统一更新，此处无需额外逻辑
        LOG.fine(() -> "heartbeat from " + session.getClientId());
    }

    /** 未 REGISTER 就发 SUBSCRIBE / PUBLISH 等：关闭连接（协议 v1 建议） */
    private boolean requireRegistered() {
        if (!session.isRegistered()) {
            reject("client must REGISTER first");
            return false;
        }
        return true;
    }

    private void reject(String reason) {
        LOG.warning(() -> "reject session " + session.getClientId() + ": " + reason);
        shouldClose = true;
    }

    private void cleanup() {
        subscriptionRegistry.removeSession(session);
        heartbeatMonitor.unregisterSession(session);
        session.close();
        LOG.info(() -> "connection closed: " + session.getClientId());
    }
}
