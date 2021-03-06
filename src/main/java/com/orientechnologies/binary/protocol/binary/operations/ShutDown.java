package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ShutDown extends Operation {

    private String username;

    private String password;

    private static final Logger LOG = LoggerFactory.getLogger(ShutDown.class);

    public ShutDown(SocketTransport transport, String username, String password) throws Exception {
        super(transport);
        this.username = username;
        this.password = password;
        opCode = Constants.REQUEST_SHUTDOWN;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        this._writeString(this.username);
        this._writeString(this.password);
    }

    @Override
    protected <T> T _read() throws Exception {
        Map<String, Object> clusterMap = this.transport.getClusterMap();
        clusterMap = new HashMap<>();
        this.getSocket().destruct();
        LOG.info("Closed Connection");
        return (T) Integer.valueOf(0);
    }
}