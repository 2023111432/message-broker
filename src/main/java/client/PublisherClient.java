package client;

import java.io.IOException;

// PublisherClient.java
public class PublisherClient extends MessageClient {
    public void publish(String topic, String message) throws IOException {
        out.println("PUBLISH " + topic + " " + message.length() + " " + message);
        String resp = in.readLine(); // 读取响应
        if (!"PUBLISHED".equals(resp)) throw new IOException("Publish failed");
    }
}