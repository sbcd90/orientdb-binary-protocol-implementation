package com.orientechnologies.binary.protocol.binary;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.binary.operations.*;
import com.orientechnologies.binary.protocol.common.AbstractTransport;
import com.orientechnologies.binary.protocol.common.ConfigurableTrait;

import java.util.List;
import java.util.Map;

public class SocketTransport extends AbstractTransport {

    private OrientSocket socket;

    private short protocolVersion;

    private int sessionId = -1;

    private boolean databaseOpened = false;

    private String databaseName;

    private String token;

    private boolean requestToken;

    private boolean connected;

    public short getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(short protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public boolean isDatabaseOpened() {
        return databaseOpened;
    }

    public void setDatabaseOpened(boolean databaseOpened) {
        this.databaseOpened = databaseOpened;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public boolean isRequestToken() {
        return requestToken;
    }

    public void setRequestToken(boolean requestToken) {
        this.requestToken = requestToken;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getToken() {
        return token;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public OrientSocket getSocket() throws Exception {
        if (this.socket == null) {
            this.socket = new OrientSocket(this.host, Integer.valueOf(this.port));
            this.protocolVersion = this.socket.connect().PROTOCOL_VERSION;
            return this.socket;
        } else {
            return this.socket;
        }
    }

    @Override
    public <T> T execute(List<Object> operations, Map<String, Object> params) {
        try {
            return this.execute(operations.get(0).toString(), params);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public ConfigurableTrait configured(Map<String, Object> options) {
        return super.configured(options);
    }

    @Override
    public void configure(Map<String, String> options) {
        this.host = options.get("host");
        this.port = options.get("port");
        this.username = options.get("username");
        this.password = options.get("password");
    }

    protected <T> T execute(String operation, Map<String, Object> params) throws Exception {

        Operation opObj = null;
        if (operation.equals("connect")) {
            opObj = new Connect(this, params.get("username").toString(), params.get("password").toString());
        }
        if (operation.equals("dbOpen")) {
            opObj = new DBOpen(this, params.get("username").toString(), params.get("password").toString());
            ((DBOpen) opObj).database = params.get("database").toString();
        }
        if (operation.equals("dbList")) {
            opObj = new DBList(this, params.get("username").toString(), params.get("password").toString());
        }
        if (operation.equals("dbClose")) {
            opObj = new DBClose(this, params.get("username").toString(), params.get("password").toString());
        }
        if (operation.equals("shutDown")) {
            opObj = new ShutDown(this, params.get("username").toString(), params.get("password").toString());
        }
        if (operation.equals("addCluster")) {
            opObj = new DataClusterAdd(this, params.get("clusterName").toString());
        }
        if (operation.equals("clusterCount")) {
            String[] clusterNames = (String[]) params.get("clusterNames");

            opObj = new DataClusterCount(this, clusterNames, (Boolean) params.get("tombstones"));
        }
        if (operation.equals("recordCreate")) {
            Record record = (Record) params.get("record");

            opObj = new RecordCreate(this, record);
        }
        if (operation.equals("recordLoad")) {
            Map<String, Object> queryOptions = params;
            opObj = new RecordLoad(this, queryOptions);
        }
        if (operation.equals("recordUpdate")) {
            Record record = (Record) params.get("record");
            RecordId rid = (RecordId) params.get("recordid");

            opObj = new RecordUpdate(this, record, rid);
        }

        Operation op = this.operationFactory(opObj, params);

        T result = op.prepare().send().getResponse();
        return result;
    }

    protected Operation operationFactory(Operation operation, Map<String, Object> params) {
        if (params.get("username") == null || params.get("username").equals("")) {
            params.put("username", this.username);
        } else {
            this.username = params.get("username").toString();
        }

        if (params.get("password") == null || params.get("password").equals("")) {
            params.put("password", this.password);
        } else {
            this.password = params.get("password").toString();
        }

        operation.configured(params);

        return operation;
    }
}