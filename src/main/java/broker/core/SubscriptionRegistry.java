package broker.core;

import broker.ClientSession;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * 订阅关系注册表：维护 topic → 订阅者 {@link ClientSession} 集合的映射。
 * <p>
 * 设计模式角色：发布/订阅模式中的「主题注册中心」，Broker 路由消息时查询此表。
 * 线程安全：{@link ConcurrentHashMap} + 每个 topic 对应一个线程安全的 Set。
 * </p>
 */
public class SubscriptionRegistry {

    private static final Logger LOG = Logger.getLogger(SubscriptionRegistry.class.getName());

    /** topic -> 订阅该 topic 的所有 ClientSession */
    private final ConcurrentHashMap<String, Set<ClientSession>> topicSubscribers;

    public SubscriptionRegistry() {
        this.topicSubscribers = new ConcurrentHashMap<>();
    }

    /**
     * 将 session 注册为 topic 的订阅者。
     */
    public void subscribe(String topic, ClientSession session) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        String normalizedTopic = topic.trim();
        topicSubscribers
                .computeIfAbsent(normalizedTopic, key -> ConcurrentHashMap.newKeySet())
                .add(session);
        session.addSubscription(normalizedTopic);
        LOG.info(() -> session.getClientId() + " subscribed to " + normalizedTopic);
    }

    /**
     * 取消 session 对 topic 的订阅。
     */
    public void unsubscribe(String topic, ClientSession session) {
        if (topic == null || topic.isBlank() || session == null) {
            return;
        }
        String normalizedTopic = topic.trim();
        Set<ClientSession> subscribers = topicSubscribers.get(normalizedTopic);
        if (subscribers != null) {
            subscribers.remove(session);
            if (subscribers.isEmpty()) {
                topicSubscribers.remove(normalizedTopic, subscribers);
            }
        }
        session.removeSubscription(normalizedTopic);
        LOG.info(() -> session.getClientId() + " unsubscribed from " + normalizedTopic);
    }

    /**
     * 查询订阅某 topic 的所有会话（用于 MessageDispatcher 路由）。
     */
    public Set<ClientSession> getSubscribers(String topic) {
        if (topic == null || topic.isBlank()) {
            return Collections.emptySet();
        }
        Set<ClientSession> subscribers = topicSubscribers.get(topic.trim());
        if (subscribers == null || subscribers.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(subscribers);
    }

    /**
     * 连接断开或心跳超时时，从所有 topic 中移除该 session。
     */
    public void removeSession(ClientSession session) {
        if (session == null) {
            return;
        }
        // 复制一份避免遍历时修改底层集合
        Set<String> topics = Set.copyOf(session.getSubscribedTopics());
        for (String topic : topics) {
            unsubscribe(topic, session);
        }
        LOG.info(() -> "removed session " + session.getClientId() + " from all topics");
    }
}
