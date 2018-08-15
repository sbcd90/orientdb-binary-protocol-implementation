package com.orientechnologies.binary.protocol.binary;

import com.orientechnologies.binary.configuration.Constants;
import org.apache.commons.lang3.ArrayUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OrientSocket {

    private boolean connected = false;

    protected short PROTOCOL_VERSION = Constants.SUPPORTED_PROTOCOL;

    private Socket socket;

    private DataInputStream in;

    private DataOutputStream out;

    private String host;

    private int port;

    private static final int CONN_TIMEOUT = 5;

    private static final int READ_TIMEOUT = 30;

    private static final int WRITE_TIMEOUT = 10;

    public OrientSocket(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public OrientSocket connect() throws Exception {
        if (!this.connected) {
            socket = new Socket(this.host, this.port);

            in = new DataInputStream(socket.getInputStream());

            out = new DataOutputStream(socket.getOutputStream());

            byte[] readValue = this.read(2);
            ByteBuffer bb = ByteBuffer.wrap(readValue);

            short protocol = bb.getShort();

            if (protocol > this.PROTOCOL_VERSION) {
                throw new RuntimeException("Version mismatch");
            }

            this.PROTOCOL_VERSION = protocol;
            this.connected = true;
        }

        return this;
    }

    public byte[] read(int size) throws Exception {

        byte[] data = new byte[]{};
        int remaining = size;

        byte[] data_ = new byte[remaining];
        in.read(data_, 0, remaining);

        data = ArrayUtils.addAll(data, data_);

        return data;
    }

    public void write(byte[] bytes) throws Exception {
        int lenOut = bytes.length;

        out.write(bytes, 0, lenOut);
    }
}