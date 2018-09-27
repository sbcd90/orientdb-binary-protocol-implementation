package com.orientechnologies.binary.protocol.binary.data;

import java.io.Serializable;

public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private short cluster = -1;

    private long position = -1;

    public RecordId(short cluster, long position) {
        this.cluster = cluster;
        this.position = position;
    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(short cluster) {
        this.cluster = cluster;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "#" + this.cluster + ":" + this.position;
    }
}