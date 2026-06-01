package broker.core;

import broker.ClientSession;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 心跳监控：定时扫描所有 {@link ClientSession} 的 {@code lastHeartbeat}，
 * 超时则关闭连接并从 {@link SubscriptionRegistry} 清理订阅。
 * <p>
 * 最低实现（见 docs/项目分工与实现计划.md）：
 * </p>
 * <ul>
 *   <li>客户端每 30s 发 HEARTBEAT</li>
 *   <li>超过 90s 无心跳则视为失效连接</li>
 * </ul>
 */
public class HeartbeatMonitor {

    /** 心跳超时阈值（毫秒），默认 90 秒 */
    private static final long HEARTBEAT_TIMEOUT_MS = 90_000L;

    /** 扫描间隔（毫秒），默认 30 秒 */
    private static final long SCAN_INTERVAL_MS = 30_000L;

    private final SubscriptionRegistry subscriptionRegistry;

    /** 所有活跃 Session，便于扫描；或由 SubscriptionRegistry / BrokerServer 维护 */
    // TODO: 引入 session 注册表，accept 时加入、断开时移除

    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public HeartbeatMonitor(SubscriptionRegistry subscriptionRegistry) {
        this.subscriptionRegistry = subscriptionRegistry;
    }

    /**
     * 启动定时扫描任务。
     */
    public void start() {
        // TODO: scheduler = Executors.newSingleThreadScheduledExecutor()
        // TODO: scheduler.scheduleAtFixedRate(this::scan, SCAN_INTERVAL_MS, SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS)
        running = true;
    }

    /**
     * 停止心跳监控。
     */
    public void stop() {
        running = false;
        // TODO: scheduler.shutdown()
    }

    /**
     * 扫描所有 Session，关闭超时连接并清理订阅。
     */
    private void scan() {
        // TODO: 遍历所有 ClientSession
        // TODO: if (now - session.getLastHeartbeatMillis() > HEARTBEAT_TIMEOUT_MS) {
        //           subscriptionRegistry.removeSession(session);
        //           session.close();
        //       }
    }

    /**
     * 新连接建立时注册到监控列表（供 scan 使用）。
     */
    public void registerSession(ClientSession session) {
        // TODO: 将 session 加入可扫描集合
    }

    /**
     * 连接正常关闭时从监控列表移除。
     */
    public void unregisterSession(ClientSession session) {
        // TODO: 从可扫描集合移除
    }
}
