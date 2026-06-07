package client;

import broker.protocol.Command;
import broker.protocol.ProtocolCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public abstract class MessageClient {
    protected Socket socket;
    protected InputStream in;
    protected OutputStream out;
    protected String clientId;

    public void connect(String host, int port) throws IOException {
        socket = new Socket(host, port);
        in = socket.getInputStream();
        out = socket.getOutputStream();
        if (clientId == null || clientId.isBlank()) {
            clientId = "client-" + System.currentTimeMillis();
        }
        sendRegister();
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public void disconnect() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    protected void sendRegister() throws IOException {
        ProtocolCodec.ProtocolFrame frame = new ProtocolCodec.ProtocolFrame();
        frame.setType(Command.REGISTER);
        frame.setClientId(clientId);
        ProtocolCodec.writeFrame(out, frame);
    }

    protected void sendSubscribe(String topic) throws IOException {
        ProtocolCodec.ProtocolFrame frame = new ProtocolCodec.ProtocolFrame();
        frame.setType(Command.SUBSCRIBE);
        frame.setTopic(topic);
        ProtocolCodec.writeFrame(out, frame);
    }

    protected void sendPublish(String topic, String payload) throws IOException {
        broker.protocol.Message message = new broker.protocol.Message(topic, payload);
        ProtocolCodec.ProtocolFrame frame = new ProtocolCodec.ProtocolFrame();
        frame.setType(Command.PUBLISH);
        frame.setMessage(message);
        ProtocolCodec.writeFrame(out, frame);
    }
}
