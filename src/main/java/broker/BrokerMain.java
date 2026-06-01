package broker;

/**
 * 消息中间件 Broker 启动入口。
 * <p>
 * 职责：加载配置（端口等）、启动 {@link BrokerServer}、注册 JVM 关闭钩子以便优雅停机。
 * </p>
 *
 * <p>设计模式角色：EDA 事件总线的启动节点，不包含业务逻辑。</p>
 */
public class BrokerMain {

    /** 默认监听端口，与 docs/项目分工与实现计划.md 一致 */
    private static final int DEFAULT_PORT = 9090;

    public static void main(String[] args) {
        int port = resolvePort(args);
        // TODO: 解析命令行 / 系统属性（broker.port）后启动服务
        broker.BrokerServer server = new broker.BrokerServer(port);
        registerShutdownHook(server);

        try {
            server.start();
        } catch (Exception e) {
            // TODO: 使用 slf4j 记录日志并退出
            System.err.println("Broker 启动失败: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 从命令行参数或系统属性中解析监听端口。
     *
     * @param args 命令行参数，可选第一位为端口号
     * @return 实际使用的端口
     */
    private static int resolvePort(String[] args) {
        // TODO: 从 args 或 System.getProperty("broker.port") 读取端口
        return DEFAULT_PORT;
    }

    /**
     * 注册 JVM 关闭钩子，在进程退出时优雅关闭 Broker。
     */
    private static void registerShutdownHook(BrokerServer server) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // TODO: 优雅关闭：停止分发线程、心跳任务、关闭 ServerSocket 与所有 Session
            server.stop();
        }, "broker-shutdown"));
    }
}
