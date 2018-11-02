package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.binary.serialization.CSV;
import com.orientechnologies.binary.protocol.common.Constants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordLoad extends Operation {

    private short clusterId;

    private long clusterPosition;

    private RecordId rid;

    private String fetchPlan = "*:0";

    private boolean ignoreCache = false;

    private boolean tombstones = false;

    public RecordLoad(SocketTransport transport, Map<String, Object> options) throws Exception {
        super(transport);
        this.clusterId =
                options.get("clusterId") != null ? Short.valueOf(options.get("clusterId").toString()): -1;
        this.clusterPosition =
                options.get("clusterPosition") != null ? Long.valueOf(options.get("clusterPosition").toString()): -1;
        this.rid = (RecordId) options.get("rid");
        opCode = Constants.RECORD_LOAD_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        if (this.rid != null && this.rid instanceof RecordId) {
            this.clusterId = (short) this.rid.getCluster();
            this.clusterPosition = this.rid.getPosition();
        }

        this._writeShort(this.clusterId);
        this._writeLong(this.clusterPosition);
        this._writeString(this.fetchPlan);
        this._writeBoolean(this.ignoreCache);
        this._writeBoolean(this.tombstones);
    }

    @Override
    protected <T> T _read() throws Exception {
        Map<String, Object> payloads = new HashMap<>();

        byte status = this._readByte();

        if (status != 0) {
            Map<String, Object> payload = new HashMap<>();
            byte type;
            int version;
            Record recordData = null;

            if (this.transport.getProtocolVersion() > 27) {
                type = this._readByte();
                version = this._readInt();
                if (type == 'b') {
                    String data = this._readString();
                } else {
                    recordData = CSV.deserialize(this._readString());
                }
            } else {
                String temp = this._readString();
                recordData = CSV.deserialize(temp);
                version = this._readInt();
                type = this._readByte();
                if (type == 'b') {
                    String finalData = temp;
                }
            }

            payload.put("rid", new RecordId(this.clusterId, (int) this.clusterPosition));
            payload.put("type", type);
            payload.put("version", version);
            if (recordData != null && recordData.getoClass() != null) {
                payload.put("oClass", recordData.getoClass());
            }
            payload.put("oData", recordData.getoData());

            List<Record> prefetchedRecords = this._readPrefetchRecord();

            Record finalRecord = new Record();
            finalRecord.setRid((RecordId) payload.get("rid"));
            finalRecord.setoClass(payload.get("oClass") != null ? payload.get("oClass").toString(): null);
            finalRecord.setVersion(Integer.valueOf(payload.get("version").toString()));
            finalRecord.setoData((Map<String, Object>) payload.get("oData"));

            return (T) finalRecord;

        }
        return null;
    }
}