package com.orientechnologies.binary.protocol.binary.data;

import java.io.Serializable;

public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private short cluster = -1;

    private int position = -1;

    public RecordId(short cluster, int position) {
        this.cluster = cluster;
        this.position = position;
    }

    public short getCluster() {
        return cluster;
    }

    public void setCluster(short cluster) {
        this.cluster = cluster;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "#" + this.cluster + ":" + this.position;
    }
}