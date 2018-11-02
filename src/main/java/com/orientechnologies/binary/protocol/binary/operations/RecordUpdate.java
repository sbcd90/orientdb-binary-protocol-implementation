package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.binary.serialization.CSV;
import com.orientechnologies.binary.protocol.common.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordUpdate extends Operation {

    private Record record;

    private short clusterId = 0;

    private int clusterPosition = 0;

    private RecordId rid;

    private boolean mode = false;

    private char recordType = Constants.RECORD_TYPE_DOCUMENT.charAt(0);

    private int recordVersion = -1;

    private int recordVersionPolicy = -1;

    private boolean updateContent = true;

    public RecordUpdate(SocketTransport transport, Record record, RecordId rid) throws Exception {
        super(transport);
        this.record = record;
        this.rid = rid;
        opCode = Constants.RECORD_UPDATE_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        if (this.rid != null) {
            this.clusterId = this.rid.getCluster();
            this.clusterPosition = this.rid.getPosition();
        }

        this.record.setRid(new RecordId(this.clusterId, this.clusterPosition));

//        this._writeInt(this.transport.getSessionId());
        this._writeShort(this.clusterId);
        this._writeLong((long) this.clusterPosition);

        if (this.transport.getProtocolVersion() >= 23) {
            this._writeBoolean(this.updateContent);
        }

        this._writeBytes(CSV.serialize(this.record));
        this._writeInt(this.recordVersion);
        this._writeByte((byte) this.recordType);
        this._writeBoolean(this.mode);
    }

    @Override
    protected <T> T _read() throws Exception {
        this.record.setVersion(this._readInt());

        if (this.transport.getProtocolVersion() > 21) {
            int changesNum = this._readInt();

            List<Map<String, Object>> changes = new ArrayList<>();
            if (changesNum > 0 && this.transport.getProtocolVersion() > 23) {
                for (int i = 0;i < changesNum;i++) {
                    Map<String, Object> change = new HashMap<>();
                    change.put("uuid-most-sig-bits", this._readLong());
                    change.put("uuid-least-sig-bits", this._readLong());
                    change.put("updated-file-id", this._readLong());
                    change.put("updated-page-index", this._readLong());
                    change.put("updated-page-offset", this._readInt());

                    changes.add(change);
                }
            }
        }
        return (T) this.record;
    }
}