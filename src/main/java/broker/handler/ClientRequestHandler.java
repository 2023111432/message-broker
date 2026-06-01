package broker.handler;

import broker.ClientSession;
import broker.core.SubscriptionRegistry;
import broker.protocol.Command;
import broker.protocol.Message;
import broker.protocol.ProtocolCodec;

import java.util.concurrent.LinkedBlockingQueue;

import static broker.protocol.Command.*;

/**
 * 客户端请求处理器：每个连接在线程池中运行的 Runnable。
 * <p>
 * 循环从 Socket 读帧 → 解析 {@link ProtocolCodec.ProtocolFrame} → 根据 {@link Command} 类型：
 * </p>
 * <ul>
 *   <li>REGISTER — 绑定 clientId</li>
 *   <li>SUBSCRIBE — 更新 {@link SubscriptionRegistry}</li>
 *   <li>UNSUBSCRIBE — 取消订阅（可选）</li>
 *   <li>PUBLISH — 将 {@link Message} 放入 {@link LinkedBlockingQueue}，由 {@link broker.core.MessageDispatcher} 消费</li>
 *   <li>HEARTBEAT — 更新 session 最后心跳时间</li>
 * </ul>
 */
public class ClientRequestHandler implements Runnable {

    private final ClientSession session;
    private final SubscriptionRegistry subscriptionRegistry;
    private final LinkedBlockingQueue<Message> outboundQueue;

    public ClientRequestHandler(ClientSession session,
                                SubscriptionRegistry subscriptionRegistry,
                                LinkedBlockingQueue<Message> outboundQueue) {
        this.session = session;
        this.subscriptionRegistry = subscriptionRegistry;
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void run() {
        try {
            // TODO: InputStream in = session.getSocket().getInputStream()
            // TODO: while (session.isOpen()) { ProtocolFrame frame = ProtocolCodec.readFrame(in); handleFrame(frame); }
        } catch (Exception e) {
            // TODO: 记录日志
        } finally {
            cleanup();
        }
    }

    /**
     * 根据命令类型分发处理逻辑。
     */
    private void handleFrame(ProtocolCodec.ProtocolFrame frame) {
        if (frame == null) {
            return;
        }
        // TODO: switch (frame.getType()) { ... }
        switch (frame.getType()) {
            case REGISTER -> handleRegister(frame);
            case SUBSCRIBE -> handleSubscribe(frame);
            case UNSUBSCRIBE -> handleUnsubscribe(frame);
            case PUBLISH -> handlePublish(frame);
            case HEARTBEAT -> handleHeartbeat(frame);
            default -> {
                // TODO: 未知命令，记录警告
            }
        }
    }

    private void handleRegister(ProtocolCodec.ProtocolFrame frame) {
        // TODO: session.setClientId(frame.getClientId())
    }

    private void handleSubscribe(ProtocolCodec.ProtocolFrame frame) {
        // TODO: subscriptionRegistry.subscribe(frame.getTopic(), session)
    }

    private void handleUnsubscribe(ProtocolCodec.ProtocolFrame frame) {
        // TODO: subscriptionRegistry.unsubscribe(frame.getTopic(), session)
    }

    private void handlePublish(ProtocolCodec.ProtocolFrame frame) {
        // TODO: outboundQueue.put(frame.getMessage()) — 异步入队，不阻塞 Publisher 过久
    }

    private void handleHeartbeat(ProtocolCodec.ProtocolFrame frame) {
        // TODO: session.touchHeartbeat()
    }

    /**
     * 连接结束时的清理：从注册表移除订阅、关闭 Session。
     */
    private void cleanup() {
        // TODO: subscriptionRegistry.removeSession(session)
        // TODO: session.close()
    }
}
