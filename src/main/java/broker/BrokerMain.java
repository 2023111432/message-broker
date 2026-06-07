package broker;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 消息中间件 Broker 启动入口。
 * <p>
 * 启动方式：
 * </p>
 * <pre>
 *   java broker.BrokerMain
 *   java broker.BrokerMain 9090
 *   java -Dbroker.port=9090 broker.BrokerMain
 * </pre>
 */
public class BrokerMain {

    private static final Logger LOG = Logger.getLogger(BrokerMain.class.getName());

    /** 默认监听端口，见 docs/协议说明.md */
    private static final int DEFAULT_PORT = 9090;

    public static void main(String[] args) {
        int port = resolvePort(args);
        BrokerServer server = new BrokerServer(port);
        registerShutdownHook(server);

        try {
            server.start();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Broker 启动失败", e);
            System.exit(1);
        }
    }

    /**
     * 端口优先级：系统属性 broker.port &gt; 命令行第一个参数 &gt; 默认值 9090。
     */
    private static int resolvePort(String[] args) {
        String property = System.getProperty("broker.port");
        if (property != null && !property.isBlank()) {
            return parsePort(property.trim(), "broker.port");
        }
        if (args.length > 0 && !args[0].isBlank()) {
            return parsePort(args[0].trim(), "command line");
        }
        return DEFAULT_PORT;
    }

    private static int parsePort(String value, String source) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port out of range: " + port);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port from " + source + ": " + value, e);
        }
    }

    /** JVM 退出时优雅关闭 Broker */
    private static void registerShutdownHook(BrokerServer server) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutdown hook triggered");
            server.stop();
        }, "broker-shutdown"));
    }
}
