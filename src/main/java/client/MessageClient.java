package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public abstract class MessageClient {
    protected Socket socket;
    protected PrintWriter out;
    protected BufferedReader in;
    protected String clientId;   // 新增：客户端唯一标识

    public void connect(String host, int port) throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // 自动生成一个默认的 clientId（可自行修改规则）
        this.clientId = socket.getLocalSocketAddress().toString() + "_" + System.currentTimeMillis();
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public void disconnect() throws IOException {
        if (socket != null) socket.close();
    }
}