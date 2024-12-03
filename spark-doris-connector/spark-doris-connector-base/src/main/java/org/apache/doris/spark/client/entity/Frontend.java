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

package org.apache.doris.spark.client.entity;

import java.io.Serializable;

public class Frontend implements Serializable {

    private String host;
    private int httpPort;
    private int queryPort;
    private int flightSqlPort;

    public Frontend(String host, int httpPort) {
        this(host, httpPort, -1, -1);
    }

    public Frontend(String host, int httpPort, int queryPort) {
        this(host, httpPort, queryPort, -1);
    }

    public Frontend(String host, int httpPort, int queryPort, int flightSqlPort) {
        this.host = host;
        this.httpPort = httpPort;
        this.queryPort = queryPort;
        this.flightSqlPort = flightSqlPort;
    }

    // Getters
    public String getHost() {
        return host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getFlightSqlPort() {
        return flightSqlPort;
    }

    public String hostHttpPortString() {
        return host + ":" + httpPort;
    }

    public String hostQueryPortString() {
        return host + ":" + queryPort;
    }

}