package client;

import java.io.IOException;

public class PublisherClient extends MessageClient {

    public void publish(String topic, String message) throws IOException {
        sendPublish(topic, message);
    }
}
