package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.configuration.Constants;
import com.orientechnologies.binary.protocol.binary.SocketTransport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBOpen extends Operation {

    protected String clientId = Constants.ID;

    protected String clientName = Constants.NAME;

    protected String clientVersion = Constants.VERSION;

    protected String serializationType = com.orientechnologies.binary.protocol.common.Constants.SERIALIZATION_DOCUMENT2CSV;

    public String database;

    private String type = com.orientechnologies.binary.protocol.common.Constants.DATABASE_TYPE_DOCUMENT;

    private String username;

    private String password;

    public DBOpen(SocketTransport transport, String username, String password) throws Exception {
        super(transport);
        this.username = username;
        this.password = password;
        opCode = com.orientechnologies.binary.protocol.common.Constants.REQUEST_DB_OPEN;
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

            this._writeString(this.database);

            if (this.transport.getProtocolVersion() < 33) {
                this._writeString(this.type);
            }

            this._writeString(username);
            this._writeString(password);
        }
    }

    @Override
    protected <T> T _read() throws Exception {
        Integer sessionId = this._readInt();
        this.transport.setSessionId(sessionId);

        this.transport.setDatabaseOpened(true);
        this.transport.setDatabaseName(this.database);
        this.transport.setConnected(false);

        if (this.transport.getProtocolVersion() > 26) {
            String token = this._readString();

            if (token == null || token.equals("")) {
                this.transport.setRequestToken(false);
            }
            this.transport.setToken(token);
        }

        short totalClusters = this._readShort();

        List<Map<String, Object>> dataCusters = new ArrayList<>();
        for (int i = 0;i < totalClusters; i++) {
            if (this.transport.getProtocolVersion() < 24) {
                Map<String, Object> params = new HashMap<>();
                params.put("name", this._readString());
                params.put("id", this._readShort());
                params.put("type", this._readString());
                params.put("dataSegment", this._readShort());
                dataCusters.add(params);
            } else {
                Map<String, Object> params = new HashMap<>();
                params.put("name", this._readString());
                params.put("id", this._readShort());
                dataCusters.add(params);
            }
        }

        Map<String, Object> clusterList = new HashMap<>();
        clusterList.put("sessionId", sessionId);
        clusterList.put("dataClusters", dataCusters);
        clusterList.put("servers", this._readString());
        clusterList.put("release", this._readString());

        this.transport.setClusterMap(clusterList);

        return (T) sessionId;
    }
}