package broker.core;

import broker.ClientSession;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * 心跳监控：定时扫描所有 {@link ClientSession} 的最后活动时间，
 * 超时则关闭连接并从 {@link SubscriptionRegistry} 清理订阅。
 * <p>
 * 协议约定：客户端每 30s 发 HEARTBEAT；超过 90s 无任何帧则断开。
 * </p>
 */
public class HeartbeatMonitor {

    private static final Logger LOG = Logger.getLogger(HeartbeatMonitor.class.getName());

    /** 心跳超时阈值（毫秒），90 秒 */
    private static final long HEARTBEAT_TIMEOUT_MS = 90_000L;

    /** 扫描间隔（毫秒），30 秒 */
    private static final long SCAN_INTERVAL_MS = 30_000L;

    private final SubscriptionRegistry subscriptionRegistry;

    /** 所有活跃 Session，accept 时加入、断开时移除 */
    private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public HeartbeatMonitor(SubscriptionRegistry subscriptionRegistry) {
        this.subscriptionRegistry = subscriptionRegistry;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::scan, SCAN_INTERVAL_MS, SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS);
        LOG.info("HeartbeatMonitor started");
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        LOG.info("HeartbeatMonitor stopped");
    }

    /** 新连接建立时注册 */
    public void registerSession(ClientSession session) {
        sessions.add(session);
    }

    /** 连接关闭时移除 */
    public void unregisterSession(ClientSession session) {
        sessions.remove(session);
    }

    /** 扫描并清理超时连接 */
    private void scan() {
        if (!running) {
            return;
        }
        long now = System.currentTimeMillis();
        for (ClientSession session : sessions) {
            if (!session.isOpen()) {
                unregisterSession(session);
                continue;
            }
            long idle = now - session.getLastHeartbeatMillis();
            if (idle > HEARTBEAT_TIMEOUT_MS) {
                LOG.warning(() -> "session timeout: " + session.getClientId()
                        + ", idle " + idle + "ms, closing");
                subscriptionRegistry.removeSession(session);
                session.close();
                unregisterSession(session);
            }
        }
    }
}
