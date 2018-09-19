package com.orientechnologies.binary.protocol.binary.operations;

import com.orientechnologies.binary.abstracts.Operation;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.common.Constants;

public class DataClusterCount extends Operation {

    private final int[] ids;

    private final boolean tombstones;

    public DataClusterCount(SocketTransport transport, String[] clusterNames, boolean tombstones) throws Exception {
        super(transport);

        this.ids = new int[clusterNames.length];

        int count = 0;
        for (String clusterName: clusterNames) {
            this.ids[count] = Integer.valueOf(this.transport.getClusterMap().get(clusterName).toString());
        }
        this.tombstones = tombstones;
        opCode = Constants.DATA_CLUSTER_COUNT_OP;
        this._writeByte((byte) this.opCode);
    }

    @Override
    protected void _write() {
        this._writeShort((short) this.ids.length);
        for (int id: this.ids) {
            this._writeShort((short) id);
        }
        this._writeBoolean(this.tombstones);
    }

    @Override
    protected <T> T _read() throws Exception {
        return (T) this._readLong();
    }
}