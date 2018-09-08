package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.common.Constants;

import java.util.Map;

public class DataClusterAdd extends Operation {

    private final short id;

    private final String clusterName;

    private final String clusterType;

    private final String location;

    private final String segmentName;

    public DataClusterAdd(SocketTransport transport, String clusterName) throws Exception {
        super(transport);
        this.id = -1;
        this.clusterName = clusterName;
        this.clusterType = Constants.CLUSTER_TYPE_PHYSICAL;
        this.location = "default";
        this.segmentName = "default";
        opCode = Constants.DATA_CLUSTER_ADD_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        if (this.transport.getProtocolVersion() < 24) {
            this._writeString(this.clusterType);
            this._writeString(this.clusterName);
            this._writeString(this.location);
            this._writeString(this.segmentName);
        } else {
            this._writeString(this.clusterName);
        }

        if (this.transport.getProtocolVersion() >= 18) {
            this._writeShort(this.id);
        }
    }

    @Override
    protected <T> T _read() throws Exception {
        Short resp = this._readShort();
        if (resp != 0) {
            Map<String, Object> clusters = this.transport.getClusterMap();
            clusters.put(this.clusterName, resp);
        }
        return (T) resp;
    }
}