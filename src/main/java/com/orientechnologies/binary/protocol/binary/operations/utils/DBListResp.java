package com.orientechnologies.binary.protocol.binary.operations.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class DBListResp {

    @JsonProperty("databases")
    private Map<String, String> databases;

    public void setDatabases(Map<String, String> databases) {
        this.databases = databases;
    }

    public Map<String, String> getDatabases() {
        return databases;
    }
}