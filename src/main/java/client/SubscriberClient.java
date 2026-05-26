package client;

import java.io.IOException;

// SubscriberClient.java
public class SubscriberClient extends MessageClient {
    private MessageListener listener;

    public void subscribe(String topic, MessageListener listener) throws IOException {
        this.listener = listener;
        out.println("SUBSCRIBE " + topic + " " + getClientId());
        String resp = in.readLine();
        if (resp.startsWith("SUBSCRIBED")) {
            // 启动接收线程
            new Thread(() -> {
                try {
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.startsWith("MSG")) {
                            String[] parts = line.split(" ", 3);
                            if (parts.length >= 3) {
                                listener.onMessage(parts[1], parts[2]);
                            }
                        }
                    }
                } catch (IOException e) { /* handle */ }
            }).start();
        }
    }

    public interface MessageListener {
        void onMessage(String topic, String payload);
    }
}