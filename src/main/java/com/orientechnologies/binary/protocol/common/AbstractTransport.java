package com.orientechnologies.binary.protocol.common;

import java.util.Map;

public abstract class AbstractTransport extends ConfigurableTrait implements ITransport {

    protected String host;

    protected String port;

    protected String username;

    protected String password;

    private Map<String, Object> clusterMap;

    private String orientVersion;

    public Map<String, Object> getClusterMap() {
        return clusterMap;
    }

    public void setClusterMap(Map<String, Object> clusterMap) {
        this.clusterMap = clusterMap;
    }


}