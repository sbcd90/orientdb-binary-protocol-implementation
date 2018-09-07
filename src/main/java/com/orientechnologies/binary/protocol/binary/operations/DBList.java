package com.orientechnologies.binary.protocol.binary.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.operations.utils.DBListResp;
import com.orientechnologies.binary.protocol.common.Constants;

public class DBList extends Operation {

    private String username;

    private String password;

    private ObjectMapper objectMapper;

    public DBList(SocketTransport transport, String username, String password) throws Exception {
        super(transport);
        this.username = username;
        this.password = password;
        this.objectMapper = new ObjectMapper();
        opCode = Constants.REQUEST_DB_LIST;
        this._writeByte((byte) this.opCode);
    }


    @Override
    protected void _write() {

    }

    @Override
    protected <T> T _read() throws Exception {
        String output = this._readSerialized();
        DBListResp dbList = this.objectMapper.readValue(("{\"" + output + "}").replace(":{", "\":{"),
                DBListResp.class);
        return (T) dbList.getDatabases();
    }
}