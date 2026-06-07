# 简易消息中间件（Message Broker）

自主实现的发布/订阅消息中间件（Java + Maven + 原生 Socket），含 Broker 服务端、客户端 SDK 与演示/测试程序。

## 快速启动（三步）

### 1. 编译

```cmd
cd C:\Users\dhdh6\Desktop\message-broker
mvn compile
```

### 2. 启动 Broker（终端 1，保持运行）

```cmd
mvn compile exec:java@broker
```

看到 `Broker listening on port 8888` 即成功。

### 3. 运行客户端程序（终端 2）

| 程序 | 命令 | 说明 |
|------|------|------|
| 联调测试 | `mvn compile exec:java@quicktest` | 验证收发 |
| 进销存演示 | `mvn compile exec:java@pssdemo` | 订单/库存/通知 |
| 性能测试 | `mvn compile exec:java@perf` | 吞吐、延迟、扇出 |

性能测试快速参数（可选）：

```cmd
mvn compile exec:java@perf -Dexec.args="2000 500 50"
```

## 文档

- [协议说明](docs/协议说明.md)
- [项目分工与实现计划](docs/项目分工与实现计划.md)
- [测试与性能报告](docs/测试与性能报告.md)

## 端口

端口说明
所有组件（Broker、客户端、演示程序）均使用 9090 端口，与服务端默认端口一致。