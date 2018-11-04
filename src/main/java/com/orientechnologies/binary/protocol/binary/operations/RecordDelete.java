package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.common.Constants;

public class RecordDelete extends Operation {

    private short clusterId = 0;

    private int clusterPosition = 0;

    private RecordId recordId;

    private boolean mode = false;

    private char recordType = Constants.RECORD_TYPE_DOCUMENT.charAt(0);

    private int recordVersion = -1;

    public RecordDelete(SocketTransport transport, RecordId recordId) throws Exception {
        super(transport);
        this.recordId = recordId;
        opCode = Constants.RECORD_DELETE_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        if (this.recordId != null) {
            this.clusterId = this.recordId.getCluster();
            this.clusterPosition = this.recordId.getPosition();
        }
        this._writeShort(this.clusterId);
        this._writeLong((long) this.clusterPosition);
        this._writeInt(this.recordVersion);
        this._writeBoolean(this.mode);

    }

    @Override
    protected <T> T _read() throws Exception {
        return (T) Boolean.valueOf(this._readBoolean());
    }
}