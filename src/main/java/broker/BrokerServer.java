package broker;

import broker.core.HeartbeatMonitor;
import broker.core.MessageDispatcher;
import broker.core.SubscriptionRegistry;
import broker.handler.ClientRequestHandler;
import broker.protocol.Message;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Broker 核心服务：事件总线 / 被观察者（EDA 中心节点）。
 */
public class BrokerServer {

    private static final Logger LOG = Logger.getLogger(BrokerServer.class.getName());

    private final int port;
    private volatile boolean running;

    private ServerSocket serverSocket;
    private ExecutorService connectionPool;

    private final SubscriptionRegistry subscriptionRegistry;
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
     * 启动 Broker：绑定端口、启动分发线程与心跳扫描，然后进入 accept 循环。
     * 此方法会阻塞当前线程，直到 {@link #stop()} 被调用。
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        AtomicInteger threadCounter = new AtomicInteger();
        connectionPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "broker-conn-" + threadCounter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        messageDispatcher.start();
        heartbeatMonitor.start();
        running = true;

        LOG.info("Broker listening on port " + port);
        acceptLoop();
    }

    /** 优雅停止 */
    public void stop() {
        running = false;

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOG.log(Level.FINE, "close ServerSocket", e);
            }
        }

        messageDispatcher.stop();
        heartbeatMonitor.stop();

        if (connectionPool != null) {
            connectionPool.shutdown();
            try {
                if (!connectionPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    connectionPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                connectionPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("Broker stopped");
    }

    /** Accept 循环：每接受一个连接就提交到线程池 */
    private void acceptLoop() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);

                ClientSession session = new ClientSession(clientSocket);
                heartbeatMonitor.registerSession(session);

                ClientRequestHandler handler = new ClientRequestHandler(
                        session,
                        subscriptionRegistry,
                        outboundQueue,
                        heartbeatMonitor
                );
                connectionPool.execute(handler);
            } catch (IOException e) {
                if (running) {
                    LOG.log(Level.WARNING, "accept failed", e);
                }
            }
        }
    }

    public SubscriptionRegistry getSubscriptionRegistry() {
        return subscriptionRegistry;
    }

    public LinkedBlockingQueue<Message> getOutboundQueue() {
        return outboundQueue;
    }
}
