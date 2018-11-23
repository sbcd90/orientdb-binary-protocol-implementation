package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.operations.utils.DataWriter;
import com.orientechnologies.binary.protocol.common.Constants;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

public class Command extends Operation {

    private char modByte = 's';

    private String command = Constants.QUERY_SYNC;

    private String query = "";

    private int limit = 20;

    private String fetchPlan = "*:0";

    public Command(SocketTransport transport, String command, String query,
                   int limit, String fetchPlan) throws Exception {
        super(transport);
        if (this.command != null) {
            this.command = command;
        }
        if (this.query != null) {
            this.query = query;
        }
        if (this.limit > 0) {
            this.limit = limit;
        }
        if (this.fetchPlan != null) {
            this.fetchPlan = fetchPlan;
        }
        opCode = Constants.COMMAND_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() throws Exception {
        if (this.command.equals(Constants.QUERY_SYNC) || this.command.equals(Constants.QUERY_CMD)
        || (this.command.equals(Constants.QUERY_GREMLIN) || this.command.equals(Constants.QUERY_SCRIPT))) {
            this.modByte = 's';
        } else {
            this.modByte = 'a';
        }
        this._writeByte((byte) this.modByte);
        byte[] payload = DataWriter.packString(this.command);

        if (this.command.equals(Constants.QUERY_SCRIPT)) {
            payload = ArrayUtils.addAll(payload, DataWriter.packString("sql"));
        }
        payload = ArrayUtils.addAll(payload, DataWriter.packString(query));
        if (this.command.equals(Constants.QUERY_SYNC) || this.command.equals(Constants.QUERY_ASYNC)
        || this.command.equals(Constants.QUERY_GREMLIN)) {
            payload = ArrayUtils.addAll(payload, DataWriter.packInt(this.limit));
            payload = ArrayUtils.addAll(payload, DataWriter.packString(this.fetchPlan));
        }
        payload = ArrayUtils.addAll(payload, DataWriter.packInt(0));
        this._writeBytes(payload);
    }

    @Override
    protected <T> T _read() throws Exception {
        if (this.command.equals(Constants.QUERY_ASYNC)) {
            return (T) this._readPrefetchRecord();
        } else {
            List<Record> records = this._readSync();
            if (this.command.equals(Constants.QUERY_CMD)) {
                if (records.size() == 1) {
                    return (T) records;
                }
            }
            return (T) records;
        }
    }
}