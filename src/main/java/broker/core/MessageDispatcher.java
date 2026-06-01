package broker.core;

import broker.ClientSession;
import broker.protocol.Message;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息分发器：独立线程从 {@link LinkedBlockingQueue} 取消息并路由到订阅者。
 * <p>
 * EDA 核心 — 「入队 → 事件循环消费 → 推送给订阅者」中的消费环节。
 * 单线程 {@code while(running) { msg = queue.take(); route(msg); }} 即可满足实验要求。
 * </p>
 */
public class MessageDispatcher implements Runnable {

    private final SubscriptionRegistry subscriptionRegistry;
    private final LinkedBlockingQueue<Message> outboundQueue;

    private volatile boolean running;
    private Thread dispatcherThread;

    public MessageDispatcher(SubscriptionRegistry subscriptionRegistry,
                             LinkedBlockingQueue<Message> outboundQueue) {
        this.subscriptionRegistry = subscriptionRegistry;
        this.outboundQueue = outboundQueue;
    }

    /**
     * 启动分发线程。
     */
    public void start() {
        // TODO: running = true; dispatcherThread = new Thread(this, "message-dispatcher"); dispatcherThread.start();
    }

    /**
     * 停止分发循环并中断阻塞在 take() 上的线程。
     */
    public void stop() {
        // TODO: running = false; 可选 offer 毒丸消息或 interrupt dispatcherThread
    }

    @Override
    public void run() {
        while (running) {
            // TODO: try {
            //     Message message = outboundQueue.take();
            //     dispatch(message);
            // } catch (InterruptedException e) {
            //     Thread.currentThread().interrupt();
            //     break;
            // }
        }
    }

    /**
     * 根据 topic 查找订阅者，向每个 {@link ClientSession} 推送 PUSH。
     */
    private void dispatch(Message message) {
        // TODO: Set<ClientSession> subscribers = subscriptionRegistry.getSubscribers(message.getTopic())
        // TODO: for (ClientSession session : subscribers) { session.push(message); }
    }
}
