package com.orientechnologies.binary.protocol.common;

import java.util.HashMap;
import java.util.Map;

public abstract class ConfigurableTrait {

    private Map<String, Object> options;

    public ConfigurableTrait() {
        this.options = new HashMap<>();
    }

    public ConfigurableTrait configured(Map<String, Object> options) {
        for (Map.Entry<String, Object> option: options.entrySet()) {
            this.options.put(option.getKey(), option.getValue());
        }

        return this;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public static ConfigurableTrait fromConfig(Map<String, Object> options) {
        ConfigurableTrait configurableTrait = new ConfigurableTrait() {};
        return configurableTrait.configured(options);
    }
}