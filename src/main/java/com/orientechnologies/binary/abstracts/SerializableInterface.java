package com.orientechnologies.binary.abstracts;

import java.io.Serializable;
import java.util.Map;

public interface SerializableInterface extends Serializable {

    static final long serialVersionUID = 1L;

    Map<String, Object> recordSerialize();
}