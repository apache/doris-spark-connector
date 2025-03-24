// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.config;

import org.apache.commons.collections.MapUtils;
import org.apache.doris.spark.exception.OptionRequiredException;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DorisConfig implements Serializable {

    private final String DORIS_REQUEST_AUTH_USER = "doris.request.auth.user";
    private final String DORIS_REQUEST_AUTH_PASSWORD = "doris.request.auth.password";

    private Map<String, String> configOptions;
    private boolean ignoreTableCheck;

    // only for test
    public DorisConfig() {
        configOptions = Collections.emptyMap();
    }

    private DorisConfig(Map<String, String> options, Boolean ignoreTableCheck) throws OptionRequiredException {
        this.configOptions = new HashMap<>(processOptions(options));
        this.ignoreTableCheck = ignoreTableCheck;
        checkOptions(this.configOptions);
    }

    private Map<String, String> processOptions(Map<String, String> options) {
        Map<String, String> dottedOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("sink.properties.") || key.startsWith("doris.sink.properties.")) {
                dottedOptions.put(key, value);
            } else {
                dottedOptions.put(key.replace('_', '.'), value);
            }
        }

        Map<String, String> processedOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : dottedOptions.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!key.startsWith("spark.")) {
                if (key.startsWith("doris.")) {
                    processedOptions.put(key, value);
                } else {
                    processedOptions.put("doris." + key, value);
                }
            }
        }
        if (processedOptions.containsKey(DORIS_REQUEST_AUTH_USER)) {
            processedOptions.put(DorisOptions.DORIS_USER.getName(), processedOptions.get(DORIS_REQUEST_AUTH_USER));
        }
        if (processedOptions.containsKey(DORIS_REQUEST_AUTH_PASSWORD)) {
            processedOptions.put(DorisOptions.DORIS_PASSWORD.getName(), processedOptions.remove(DORIS_REQUEST_AUTH_PASSWORD));
        }
        return processedOptions;
    }

    private void checkOptions(Map<String, String> options) throws OptionRequiredException {
        if (!options.containsKey(DorisOptions.DORIS_FENODES.getName())) {
            throw new OptionRequiredException(DorisOptions.DORIS_FENODES.getName());
        } else {
            String feNodes = options.get(DorisOptions.DORIS_FENODES.getName());
            if (feNodes.isEmpty()) {
                throw new IllegalArgumentException("option [" + DorisOptions.DORIS_FENODES.getName() + "] is empty");
            } else if (feNodes.contains(":")) {
                if (!feNodes.matches("([\\w-.]+:\\d+,)*([\\w-.]+:\\d+)")) {
                    throw new IllegalArgumentException("option [" + DorisOptions.DORIS_FENODES.getName() + "] is not in correct format, for example: host:port[,host2:port]");
                }
            }
        }
        if (!ignoreTableCheck) {
            if (!options.containsKey(DorisOptions.DORIS_TABLE_IDENTIFIER.getName())) {
                throw new OptionRequiredException(DorisOptions.DORIS_TABLE_IDENTIFIER.getName());
            } else {
                String tableIdentifier = options.get(DorisOptions.DORIS_TABLE_IDENTIFIER.getName());
                if (tableIdentifier.isEmpty()) {
                    throw new IllegalArgumentException("option [" + DorisOptions.DORIS_TABLE_IDENTIFIER.getName() + "] is empty");
                } else if (!tableIdentifier.contains(".")) {
                    throw new IllegalArgumentException("option [" + DorisOptions.DORIS_TABLE_IDENTIFIER.getName() + "] is not in correct format, for example: db.table");
                }
            }
        }
        if (!options.containsKey(DorisOptions.DORIS_USER.getName())) {
            throw new OptionRequiredException(DorisOptions.DORIS_USER.getName());
        } else if (options.get(DorisOptions.DORIS_USER.getName()).isEmpty()) {
            throw new IllegalArgumentException("option [" + DorisOptions.DORIS_USER.getName() + "] is empty");
        }
        if (!options.containsKey(DorisOptions.DORIS_PASSWORD.getName())) {
            throw new OptionRequiredException(DorisOptions.DORIS_PASSWORD.getName());
        }
        if ("thrift".equalsIgnoreCase(options.get(DorisOptions.READ_MODE.getName()))) {
            if (Boolean.parseBoolean(options.get(DorisOptions.DORIS_READ_BITMAP_TO_STRING.getName()))) {
                throw new IllegalArgumentException(String.format("option [%s] is invalid in thrift read mode",
                        DorisOptions.DORIS_READ_BITMAP_TO_STRING.getName()));
            }
            if (Boolean.parseBoolean(options.get(DorisOptions.DORIS_READ_BITMAP_TO_BASE64.getName()))) {
                throw new IllegalArgumentException(String.format("option [%s] is invalid in thrift read mode",
                        DorisOptions.DORIS_READ_BITMAP_TO_BASE64.getName()));
            }
        } else {
            if (Boolean.parseBoolean(options.get(DorisOptions.DORIS_READ_BITMAP_TO_STRING.getName()))
                    && Boolean.parseBoolean(options.get(DorisOptions.DORIS_READ_BITMAP_TO_BASE64.getName()))) {
                throw new IllegalArgumentException("option [%s] and [%s] cannot be true at this same time");
            }
        }
    }

    public boolean contains(ConfigOption<?> option) {
        return configOptions.containsKey(option.getName());
    }

    public <T> T getValue(ConfigOption<T> option) throws OptionRequiredException {
        if (configOptions.containsKey(option.getName())) {
            String originVal = configOptions.get(option.getName());
            Class<?> typeClass = option.getTypeClass();
            if (typeClass == Integer.class) {
                return (T) typeClass.cast(Integer.parseInt(originVal));
            } else if (typeClass == Long.class) {
                return (T) typeClass.cast(Long.parseLong(originVal));
            } else if (typeClass == Double.class) {
                return (T) typeClass.cast(Double.parseDouble(originVal));
            } else if (typeClass == Boolean.class) {
                return (T) typeClass.cast(Boolean.parseBoolean(originVal));
            } else if (typeClass == String.class) {
                return (T) originVal;
            } else {
                throw new IllegalArgumentException("Unsupported config value type: " + typeClass);
            }
        } else if (option.getDefaultValue() != null) {
            return option.getDefaultValue();
        } else {
            throw new OptionRequiredException(option.getName());
        }
    }

    public void setProperty(ConfigOption<?> option, String value) throws OptionRequiredException {
        configOptions.put(option.getName(), value);
        configOptions = processOptions(configOptions);
        checkOptions(configOptions);
    }

    public Map<String, String> getSinkProperties() {
        Map<String, String> sinkProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : configOptions.entrySet()) {
            if (entry.getKey().startsWith(DorisOptions.STREAM_LOAD_PROP_PREFIX)) {
                sinkProperties.put(entry.getKey().substring(DorisOptions.STREAM_LOAD_PROP_PREFIX.length()), entry.getValue());
            }
        }
        return sinkProperties;
    }

    public void merge(Map<String, String> map) throws OptionRequiredException {
        configOptions.putAll(map);
        configOptions = processOptions(configOptions);
        checkOptions(configOptions);
    }

    public Map<String, String> toMap() {
        return new HashMap<>(configOptions);
    }

    public static DorisConfig fromMap(Map<String, String> sparkConfMap, Boolean ignoreTableCheck) throws OptionRequiredException {
        return fromMap(sparkConfMap, Collections.emptyMap(), ignoreTableCheck);
    }

    public static DorisConfig fromMap(Map<String, String> sparkConfMap, Map<String, String> options, Boolean ignoreTableCheck) throws OptionRequiredException {
        Map<String, String> map = new HashMap<>(sparkConfMap);
        if (MapUtils.isNotEmpty(options)) {
            map.putAll(options);
        }
        return new DorisConfig(map, ignoreTableCheck);
    }

}