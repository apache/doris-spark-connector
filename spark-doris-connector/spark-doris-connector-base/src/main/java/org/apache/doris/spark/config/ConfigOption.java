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

public class ConfigOption<T> {

    private final String name;
    private Class<?> tClass;
    private T defaultValue;
    private String description;

    public ConfigOption(String name, Class<?> tClass, T defaultValue) {
        this.name = name;
        this.tClass = tClass;
        this.defaultValue = defaultValue;
    }

    public ConfigOption<T> withDescription(String desc) {
        this.description = desc;
        return this;
    }

    public String getName() {
        return name;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public Class<?> getTypeClass() {
        return tClass;
    }
}