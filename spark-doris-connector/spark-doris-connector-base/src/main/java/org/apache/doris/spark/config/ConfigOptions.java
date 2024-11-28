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

public class ConfigOptions {

    public static ConfigOptionBuilder name(String name) {
        return new ConfigOptionBuilder(name);
    }

    public static class ConfigOptionBuilder {

        private final String name;

        public ConfigOptionBuilder(String name) {
            this.name = name;
        }

        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(name, Integer.class);
        }

        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(name, Long.class);
        }

        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(name, Double.class);
        }

        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(name, Boolean.class);
        }

        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(name, String.class);
        }

    }

    public static class TypedConfigOptionBuilder<T> {
        private final String name;
        private final Class<T> tClass;

        public TypedConfigOptionBuilder(String name, Class<T> tClass) {
            this.name = name;
            this.tClass = tClass;
        }

        public ConfigOption<T> defaultValue(T defaultValue) {
            return new ConfigOption<>(name, tClass, defaultValue);
        }

        public ConfigOption<T> withoutDefaultValue() {
            return new ConfigOption<>(name, tClass, null);
        }

    }

}
