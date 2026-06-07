package client;

import broker.protocol.Command;
import broker.protocol.ProtocolCodec;

import java.io.IOException;

public class SubscriberClient extends MessageClient {

    public void subscribe(String topic, MessageListener listener) throws IOException {
        sendSubscribe(topic);
        Thread receiver = new Thread(() -> {
            try {
                while (socket != null && socket.isConnected() && !socket.isClosed()) {
                    ProtocolCodec.ProtocolFrame frame = ProtocolCodec.readFrame(in);
                    if (frame == null) {
                        break;
                    }
                    if (frame.getType() == Command.PUSH && frame.getMessage() != null) {
                        var msg = frame.getMessage();
                        listener.onMessage(msg.getTopic(), msg.getPayload());
                    }
                }
            } catch (IOException ignored) {
                // connection closed
            }
        }, "subscriber-recv-" + getClientId());
        receiver.setDaemon(true);
        receiver.start();
    }

    public interface MessageListener {
        void onMessage(String topic, String payload);
    }
}
