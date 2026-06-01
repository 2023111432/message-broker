package broker.core;

import broker.ClientSession;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 订阅关系注册表：维护 topic → 订阅者 {@link ClientSession} 集合的映射。
 * <p>
 * 设计模式角色：发布/订阅模式中的「主题注册中心」，Broker 路由消息时查询此表。
 * 线程安全：使用 {@link ConcurrentHashMap} + {@link CopyOnWriteArraySet}（或等价结构）。
 * </p>
 */
public class SubscriptionRegistry {

    /** topic -> 订阅该 topic 的所有 ClientSession */
    private final ConcurrentHashMap<String, Set<ClientSession>> topicSubscribers;

    public SubscriptionRegistry() {
        this.topicSubscribers = new ConcurrentHashMap<>();
    }

    /**
     * 将 session 注册为 topic 的订阅者。
     *
     * @param topic   主题名
     * @param session 客户端会话
     */
    public void subscribe(String topic, ClientSession session) {
        // TODO: topicSubscribers.computeIfAbsent(...).add(session)
        // TODO: session.addSubscription(topic) 保持双向一致
    }

    /**
     * 取消 session 对 topic 的订阅。
     */
    public void unsubscribe(String topic, ClientSession session) {
        // TODO: 从 topicSubscribers 移除 session，空集合时可清理 key
        // TODO: session.removeSubscription(topic)
    }

    /**
     * 查询订阅某 topic 的所有会话（用于 MessageDispatcher 路由）。
     *
     * @return 订阅者集合快照，无订阅者时返回空集合
     */
    public Set<ClientSession> getSubscribers(String topic) {
        // TODO: 返回 topicSubscribers.getOrDefault 的不可变视图
        Set<ClientSession> subscribers = topicSubscribers.get(topic);
        if (subscribers == null || subscribers.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(subscribers);
    }

    /**
     * 连接断开或心跳超时时，从所有 topic 中移除该 session。
     */
    public void removeSession(ClientSession session) {
        // TODO: 遍历 session.getSubscribedTopics()，逐个 unsubscribe
        // TODO: 或遍历 topicSubscribers 清理包含该 session 的条目
    }
}
