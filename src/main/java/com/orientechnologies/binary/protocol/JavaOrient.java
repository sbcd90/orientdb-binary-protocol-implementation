package com.orientechnologies.binary.protocol;

import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.common.Constants;
import com.orientechnologies.binary.protocol.common.ITransport;

import java.util.Arrays;
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

    public void connect(String username, String password) {
        String serializationType = Constants.SERIALIZATION_DOCUMENT2CSV;

        Map<String, String> params = new HashMap<>();
        params.put("username", username);
        params.put("password", password);
        params.put("serializationType", serializationType);

        this.getTransport().execute(Arrays.asList("connect"), params);
    }

    public void dbOpen(String database, String username, String password) {
        Map<String, String> params = new HashMap<>();
        params.put("databaseType", Constants.DATABASE_TYPE_DOCUMENT);
        params.put("serializationType", Constants.SERIALIZATION_DOCUMENT2CSV);

        Map<String, String> values = new HashMap<>();
        values.put("database", database);
        values.put("type", params.get("databaseType"));
        values.put("username", username);
        values.put("password", password);
        values.put("serializationType", Constants.SERIALIZATION_DOCUMENT2CSV);

        this.transport.execute(Arrays.asList("dbOpen"), values);
    }

    public void dbList(String username, String password) {
        Map<String, String> params = new HashMap<>();
        params.put("username", username);
        params.put("password", password);

        this.transport.execute(Arrays.asList("dbList"), params);
    }

    public static void main(String[] args) {
        JavaOrient javaOrient = new JavaOrient("127.0.0.1", "2424");
        javaOrient.username = "root";
        javaOrient.password = "root";

        javaOrient.connect("root", "root");
        javaOrient.dbList("root", "root");
        javaOrient.dbOpen("GratefulDeadConcerts", "root", "root");
    }
}