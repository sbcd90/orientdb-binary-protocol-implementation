package com.orientechnologies.binary.protocol.common;

import java.util.HashMap;
import java.util.Map;

public abstract class ConfigurableTrait {

    private Map<String, String> options;

    public ConfigurableTrait() {
        this.options = new HashMap<>();
    }

    public ConfigurableTrait configured(Map<String, String> options) {
        for (Map.Entry<String, String> option: options.entrySet()) {
            this.options.put(option.getKey(), option.getValue());
        }

        return this;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public static ConfigurableTrait fromConfig(Map<String, String> options) {
        ConfigurableTrait configurableTrait = new ConfigurableTrait() {};
        return configurableTrait.configured(options);
    }
}