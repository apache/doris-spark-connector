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

package org.apache.doris.spark.client.read;

import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class DorisReadModeResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DorisReadModeResolver.class);
    private static final String ARROW = "arrow";
    private static final String THRIFT = "thrift";

    private DorisReadModeResolver() {
    }

    public static String resolve(DorisConfig config) throws Exception {
        return resolve(config, () -> discoverPort(new DorisFrontendClient(config)));
    }

    static String resolve(DorisConfig config, DorisFrontendClient frontendClient) throws Exception {
        return resolve(config, () -> discoverPort(frontendClient));
    }

    private static int discoverPort(DorisFrontendClient frontendClient) throws Exception {
        try {
            return frontendClient.tryGetArrowFlightSqlPort();
        } finally {
            frontendClient.close();
        }
    }

    static String resolve(DorisConfig config, FlightSqlPortSupplier portSupplier) throws Exception {
        String readMode = config.getValue(DorisOptions.READ_MODE).toLowerCase(Locale.ROOT);
        if (THRIFT.equals(readMode)) {
            return THRIFT;
        }
        if (!ARROW.equals(readMode)) {
            throw new IllegalArgumentException("Unknown read mode: " + readMode);
        }

        int flightSqlPort = config.getValue(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT);
        if (flightSqlPort > 0) {
            return ARROW;
        }

        try {
            flightSqlPort = portSupplier.get();
        } catch (Exception e) {
            LOG.warn("failed to discover Arrow Flight SQL port, falling back to Thrift", e);
            return THRIFT;
        }
        if (flightSqlPort > 0) {
            config.setProperty(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT, String.valueOf(flightSqlPort));
            LOG.info("use Arrow Flight SQL to read data, port is {}", flightSqlPort);
            return ARROW;
        }

        LOG.warn("Arrow Flight SQL port {} is invalid or unavailable, falling back to Thrift", flightSqlPort);
        return THRIFT;
    }

    @FunctionalInterface
    interface FlightSqlPortSupplier {
        int get() throws Exception;
    }
}
