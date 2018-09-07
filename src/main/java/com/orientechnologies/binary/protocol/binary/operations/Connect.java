package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.configuration.Constants;
import com.orientechnologies.binary.protocol.binary.SocketTransport;

public class Connect extends Operation {

    protected String clientId = Constants.ID;

    public String clientName = Constants.NAME;

    public String clientVersion = Constants.VERSION;

    public String serializationType = com.orientechnologies.binary.protocol.common.Constants.SERIALIZATION_DOCUMENT2CSV;

    public String username;

    public String password;

    public Connect(SocketTransport transport, String username, String password) throws Exception {
        super(transport);
        this.username = username;
        this.password = password;
        opCode = com.orientechnologies.binary.protocol.common.Constants.REQUEST_CONNECT;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        this._writeString(this.clientName);
        this._writeString(this.clientVersion);
        this._writeShort(this.transport.getProtocolVersion());

        if (this.transport.getProtocolVersion() > 21) {
            this._writeString(this.clientId);
            this._writeString(this.serializationType);

            if (this.transport.getProtocolVersion() > 26) {
                this._writeBoolean(this.transport.isRequestToken());
                if (this.transport.getProtocolVersion() >= 36) {
                    this._writeBoolean(true);
                    this._writeBoolean(true);
                }
            }

            this._writeString(username);
            this._writeString(password);
        } else {
            this._writeString(this.clientId);
            this._writeString(username);
            this._writeString(password);
        }
    }

    @Override
    protected <T> T _read() throws Exception {
        Integer sessionId = this._readInt();
        this.transport.setSessionId(sessionId);

        if (this.transport.getProtocolVersion() > 26) {
            String token = this._readString();
            if (token == null || token.equals("")) {
                this.transport.setRequestToken(false);
            }
            this.transport.setToken(token);
        }

        this.transport.setConnected(true);

        return (T) sessionId;
    }
}