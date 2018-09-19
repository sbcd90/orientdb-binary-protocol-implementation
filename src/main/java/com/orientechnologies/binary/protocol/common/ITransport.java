package com.orientechnologies.binary.protocol.common;

import java.util.List;
import java.util.Map;

public interface ITransport extends IConfigurable {

    public <T> T execute(List<Object> operations, Map<String, Object> params);
}