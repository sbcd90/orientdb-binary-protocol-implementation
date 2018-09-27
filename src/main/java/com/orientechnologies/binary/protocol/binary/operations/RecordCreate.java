package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.binary.serialization.CSV;
import com.orientechnologies.binary.protocol.common.Constants;

import java.util.HashMap;
import java.util.Map;

public class RecordCreate extends Operation {

    private short clusterId = 0;

    private int segment = -1;

    private Record record;

    private char recordType = Constants.RECORD_TYPE_DOCUMENT.charAt(0);

    private int mode = 0;

    public RecordCreate(SocketTransport transport, Record record) throws Exception {
        super(transport);
        this.record = record;
        opCode = Constants.RECORD_CREATE_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        if (this.transport.getProtocolVersion() < 24) {
            this._writeInt(this.segment);
        }

        this._writeShort(this.clusterId);
        this._writeBytes(CSV.serialize(record));

        this._writeByte((byte) this.recordType);
        this._writeBoolean(this.mode == 0);
    }

    @Override
    protected <T> T _read() throws Exception {
        short clusterId = this.clusterId;
        if (this.transport.getProtocolVersion() > 25) {
            clusterId = this._readShort();
        }

        long position = this._readLong();
        int version = this._readInt();

        if (this.transport.getProtocolVersion() > 21) {
            int changesNum = this._readInt();
            Map<String, Object> changes = new HashMap<>();
            if (changesNum > 0 && this.transport.getProtocolVersion() > 23) {
                for (int i=0;i < changesNum;i++) {
                    changes.put("uuid-most-sig-bits", this._readLong());
                    changes.put("uuid-least-sig-bits", this._readLong());
                    changes.put("updated-file-id", this._readLong());
                    changes.put("updated-page-index", this._readLong());
                    changes.put("updated-page-offset", this._readInt());
                }
            }

            if (this.record != null) {
                this.record.setRid(new RecordId(clusterId, position));
                this.record.setVersion(version);
            }
        }
        return (T) this.record;
    }
}