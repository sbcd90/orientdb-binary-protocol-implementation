package com.orientechnologies.binary.protocol;

import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.common.AbstractTransport;
import com.orientechnologies.binary.protocol.common.Constants;
import com.orientechnologies.binary.protocol.common.ITransport;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JavaOrient {

    private final String host;

    private final String port;

    private String username;

    private String password;

    private ITransport transport;

    public JavaOrient(String host, String port) {
        if (host.equals("localhost")) {
            host = "127.0.0.1";
        }
        if (port == null) {
            port = "2424";
        }

        this.host = host;
        this.port = port;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public JavaOrient setTransport(ITransport transport) {
        this.transport = this.createTransport(transport);
        return this;
    }

    public ITransport getTransport() {
        if (this.transport == null) {
            this.transport = this.createTransport(null);
        }
        return this.transport;
    }

    protected ITransport createTransport(ITransport transport) {
        if (transport == null) {
            transport = new SocketTransport();
        }

        Map<String, String> options = new HashMap<>();
        options.put("host", this.host);
        options.put("port", this.port);
        options.put("username", this.username);
        options.put("password", this.password);
        transport.configure(options);

        return transport;
    }

    public <T> T connect(String username, String password) {
        String serializationType = Constants.SERIALIZATION_DOCUMENT2CSV;

        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("password", password);
        params.put("serializationType", serializationType);

        return this.getTransport().execute(Arrays.asList("connect"), params);
    }

    public <T> T dbOpen(String database, String username, String password) {
        Map<String, Object> params = new HashMap<>();
        params.put("databaseType", Constants.DATABASE_TYPE_DOCUMENT);
        params.put("serializationType", Constants.SERIALIZATION_DOCUMENT2CSV);

        Map<String, Object> values = new HashMap<>();
        values.put("database", database);
        values.put("type", params.get("databaseType"));
        values.put("username", username);
        values.put("password", password);
        values.put("serializationType", Constants.SERIALIZATION_DOCUMENT2CSV);

        return this.transport.execute(Arrays.asList("dbOpen"), values);
    }

    public <T> T dbList(String username, String password) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("password", password);

        return this.transport.execute(Arrays.asList("dbList"), params);
    }

    public <T> T dbClose(String username, String password) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("password", password);

        return this.transport.execute(Arrays.asList("dbClose"), params);
    }

    public <T> T shutDown(String username, String password) {
        Map<String, Object> params = new HashMap<>();
        params.put("username", username);
        params.put("password", password);

        return this.transport.execute(Arrays.asList("shutDown"), params);
    }

    public <T> T addCluster(String clusterName) {
        Map<String, Object> params = new HashMap<>();
        params.put("clusterName", clusterName);

        return this.transport.execute(Arrays.asList("addCluster"), params);
    }

    public Integer getCluster(String clusterName) {
        if (this.transport instanceof AbstractTransport) {
            return Integer.valueOf(((AbstractTransport) this.transport)
                    .getClusterMap().get(clusterName).toString());
        }
        return -1;
    }

    public <T> T getClusterCount(String[] clusterNames, boolean tombstones) {
        Map<String, Object> params = new HashMap<>();
        params.put("clusterNames", clusterNames);
        params.put("tombstones", tombstones);

        return this.transport.execute(Arrays.asList("clusterCount"), params);
    }

    public <T> T createRecord(Record record) {
        Map<String, Object> params = new HashMap<>();
        params.put("record", record);

        return this.transport.execute(Arrays.asList("recordCreate"), params);
    }

    public <T> T readRecord(short clusterId, long clusterPosition, RecordId recordId) {
        Map<String, Object> queryOptions = new HashMap<>();
        queryOptions.put("clusterId", clusterId);
        queryOptions.put("clusterPosition", clusterPosition);
        queryOptions.put("rid", recordId);
        return this.transport.execute(Arrays.asList("recordLoad"), queryOptions);
    }

    public <T> T updateRecord(Record record, RecordId rid) {
        Map<String, Object> params = new HashMap<>();
        params.put("record", record);
        params.put("recordid", rid);

        return this.transport.execute(Arrays.asList("recordUpdate"), params);
    }

    public <T> T deleteRecord(RecordId rid) {
        Map<String, Object> params = new HashMap<>();
        params.put("recordid", rid);

        return this.transport.execute(Arrays.asList("recordDelete"), params);
    }

    public <T> T readRecords(String command, String query, int limit, String fetchPlan) {
        Map<String, Object> params = new HashMap<>();
        params.put("command", command);
        params.put("query", query);
        params.put("limit", limit);
        params.put("fetchPlan", fetchPlan);

        return this.transport.execute(Arrays.asList("commandLoad"), params);
    }
}