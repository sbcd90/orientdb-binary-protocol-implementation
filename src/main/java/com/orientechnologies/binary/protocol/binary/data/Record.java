package com.orientechnologies.binary.protocol.binary.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.binary.abstracts.SerializableInterface;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Record implements SerializableInterface {

    private RecordId rid;

    private String oClass;

    private int version;

    private Map<String, Object> oData;

    public RecordId getRid() {
        return rid;
    }

    public void setRid(RecordId rid) {
        this.rid = rid;
    }

    public Record withRid(RecordId rid) {
        this.rid = rid;
        return this;
    }

    public String getoClass() {
        return oClass;
    }

    public void setoClass(String oClass) {
        this.oClass = oClass;
    }

    public Record withoClass(String oClass) {
        this.oClass = oClass;
        return this;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public Record withVersion(int version) {
        this.version = version;
        return this;
    }

    public void setoData(Map<String, Object> oData) {
        this.oData = oData;
    }

    public Map<String, Object> getoData() {
        return oData;
    }

    public Record withoData(Map<String, Object> oData) {
        this.oData = oData;
        return this;
    }

    @Override
    public Map<String, Object> recordSerialize() {
        return jsonSerialize();
    }

    public Map<String, Object> jsonSerialize() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("rid", this.getRid());
        meta.put("version", this.getVersion());
        meta.put("oClass", this.getoClass());
        meta.put("oData", this.getoData());
        return meta;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}