package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.common.Constants;

public class DBList extends Operation {

    private String username;

    private String password;

    public DBList(SocketTransport transport) throws Exception {
        super(transport);
        this.username = "root";
        this.password = "root";
        opCode = Constants.REQUEST_DB_LIST;
    }


    @Override
    protected void _write() {

    }

    @Override
    protected int _read() throws Exception {
        String output = this._readSerialized();
        return 0;
    }
}