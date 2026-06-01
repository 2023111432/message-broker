package broker;

import broker.core.HeartbeatMonitor;
import broker.core.MessageDispatcher;
import broker.core.SubscriptionRegistry;
import broker.handler.ClientRequestHandler;
import broker.protocol.Message;

import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Broker 核心服务：事件总线 / 被观察者（EDA 中心节点）。
 * <p>
 * 维护 {@link ServerSocket} 接受客户端长连接，将连接交给线程池处理；
 * 持有订阅注册表、待分发消息队列，并驱动 {@link MessageDispatcher} 与 {@link HeartbeatMonitor}。
 * </p>
 *
 * <p>线程模型（简化版）：</p>
 * <ul>
 *   <li>Accept 线程：循环 {@code accept()}，将新连接提交线程池</li>
 *   <li>连接读线程（池内任务）：解析命令、更新注册表、消息入队</li>
 *   <li>{@link MessageDispatcher}：单线程消费 {@link LinkedBlockingQueue} 并路由</li>
 *   <li>{@link HeartbeatMonitor}：定时扫描超时连接并清理订阅</li>
 * </ul>
 */
public class BrokerServer {

    private final int port;
    private volatile boolean running;

    private ServerSocket serverSocket;
    private ExecutorService connectionPool;

    /** 主题 → 订阅者映射，线程安全 */
    private final SubscriptionRegistry subscriptionRegistry;

    /** 待分发消息队列，Publisher 发布后经 Handler 入队，由 Dispatcher 消费 */
    private final LinkedBlockingQueue<Message> outboundQueue;

    private final MessageDispatcher messageDispatcher;
    private final HeartbeatMonitor heartbeatMonitor;

    public BrokerServer(int port) {
        this.port = port;
        this.subscriptionRegistry = new SubscriptionRegistry();
        this.outboundQueue = new LinkedBlockingQueue<>();
        this.messageDispatcher = new MessageDispatcher(subscriptionRegistry, outboundQueue);
        this.heartbeatMonitor = new HeartbeatMonitor(subscriptionRegistry);
    }

    /**
     * 启动 Broker：绑定端口、启动分发线程与心跳扫描。
     */
    public void start() throws Exception {
        // TODO: serverSocket = new ServerSocket(port)
        // TODO: connectionPool = Executors.newCachedThreadPool() 或固定大小线程池
        // TODO: messageDispatcher.start()
        // TODO: heartbeatMonitor.start()
        running = true;

        // TODO: accept 循环 — 每接受一个 Socket 创建 ClientSession 并提交 ClientRequestHandler
        acceptLoop();
    }

    /**
     * 优雅停止：置 running=false、关闭资源。
     */
    public void stop() {
        running = false;
        // TODO: 关闭 serverSocket、shutdown 线程池、停止 dispatcher 与 heartbeat
        if (connectionPool != null) {
            connectionPool.shutdown();
            try {
                connectionPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 主 Accept 循环：接受客户端连接并交给线程池处理。
     */
    private void acceptLoop() {
        while (running) {
            // TODO: Socket client = serverSocket.accept()
            // TODO: ClientSession session = new ClientSession(client)
            // TODO: ClientRequestHandler handler = new ClientRequestHandler(session, subscriptionRegistry, outboundQueue)
            // TODO: connectionPool.execute(handler)
        }
    }

    public SubscriptionRegistry getSubscriptionRegistry() {
        return subscriptionRegistry;
    }

    public LinkedBlockingQueue<Message> getOutboundQueue() {
        return outboundQueue;
    }
}
