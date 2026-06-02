package broker.core;

import broker.ClientSession;
import broker.protocol.Message;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 消息分发器：独立线程从 {@link LinkedBlockingQueue} 取消息并路由到订阅者。
 * <p>
 * EDA 核心 — 「入队 → 事件循环消费 → 推送给订阅者」中的消费环节。
 * </p>
 */
public class MessageDispatcher implements Runnable {

    private static final Logger LOG = Logger.getLogger(MessageDispatcher.class.getName());

    /** 停止分发线程时入队的毒丸消息（仅内部使用） */
    private static final Message POISON_PILL = new Message("__POISON__", "");

    private final SubscriptionRegistry subscriptionRegistry;
    private final LinkedBlockingQueue<Message> outboundQueue;

    private volatile boolean running;
    private Thread dispatcherThread;

    public MessageDispatcher(SubscriptionRegistry subscriptionRegistry,
                             LinkedBlockingQueue<Message> outboundQueue) {
        this.subscriptionRegistry = subscriptionRegistry;
        this.outboundQueue = outboundQueue;
    }

    /** 启动分发线程 */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        dispatcherThread = new Thread(this, "message-dispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();
        LOG.info("MessageDispatcher started");
    }

    /** 停止分发循环 */
    public void stop() {
        running = false;
        outboundQueue.offer(POISON_PILL);
        if (dispatcherThread != null) {
            dispatcherThread.interrupt();
        }
        LOG.info("MessageDispatcher stopped");
    }

    @Override
    public void run() {
        while (running) {
            try {
                Message message = outboundQueue.take();
                if (isPoisonPill(message)) {
                    break;
                }
                dispatch(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private boolean isPoisonPill(Message message) {
        return message != null && POISON_PILL.getTopic().equals(message.getTopic());
    }

    /**
     * 根据 topic 查找订阅者，向每个 {@link ClientSession} 推送 PUSH。
     */
    private void dispatch(Message message) {
        if (message == null || message.getTopic() == null || message.getTopic().isBlank()) {
            LOG.warning("skip dispatch: invalid message");
            return;
        }

        Set<ClientSession> subscribers = subscriptionRegistry.getSubscribers(message.getTopic());
        if (subscribers.isEmpty()) {
            LOG.fine(() -> "no subscribers for topic " + message.getTopic());
            return;
        }

        for (ClientSession session : subscribers) {
            try {
                session.push(message);
            } catch (Exception e) {
                LOG.log(Level.WARNING, "dispatch to " + session.getClientId() + " failed", e);
            }
        }
        LOG.fine(() -> "dispatched to " + subscribers.size() + " subscriber(s), topic=" + message.getTopic());
    }
}
