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

import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DorisReadModeResolverTest {

    @Test
    public void readOptionsDefaultToArrowWithInvalidFlightSqlPort() {
        Assert.assertEquals("arrow", DorisOptions.READ_MODE.getDefaultValue());
        Assert.assertEquals(Integer.valueOf(-1), DorisOptions.DORIS_READ_FLIGHT_SQL_PORT.getDefaultValue());
    }

    @Test
    public void defaultArrowModeDiscoversFlightSqlPort() throws Exception {
        DorisConfig config = createConfig(null, null);

        String mode = DorisReadModeResolver.resolve(config, () -> 9040);

        Assert.assertEquals("arrow", mode);
        Assert.assertEquals(Integer.valueOf(9040), config.getValue(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT));
    }

    @Test
    public void explicitThriftSkipsPortDiscovery() throws Exception {
        DorisConfig config = createConfig("thrift", null);
        AtomicBoolean discovered = new AtomicBoolean(false);

        String mode = DorisReadModeResolver.resolve(config, () -> {
            discovered.set(true);
            return 9040;
        });

        Assert.assertEquals("thrift", mode);
        Assert.assertFalse(discovered.get());
    }

    @Test
    public void configuredPositivePortKeepsArrowMode() throws Exception {
        DorisConfig config = createConfig("arrow", 9040);
        AtomicBoolean discovered = new AtomicBoolean(false);

        String mode = DorisReadModeResolver.resolve(config, () -> {
            discovered.set(true);
            return 9050;
        });

        Assert.assertEquals("arrow", mode);
        Assert.assertFalse(discovered.get());
    }

    @Test
    public void discoveredPositivePortKeepsArrowModeAndUpdatesConfig() throws Exception {
        DorisConfig config = createConfig("arrow", null);

        String mode = DorisReadModeResolver.resolve(config, () -> 9040);

        Assert.assertEquals("arrow", mode);
        Assert.assertEquals(Integer.valueOf(9040), config.getValue(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT));
    }

    @Test
    public void invalidDiscoveredPortFallsBackToThrift() throws Exception {
        DorisConfig config = createConfig("arrow", null);

        String mode = DorisReadModeResolver.resolve(config, () -> -1);

        Assert.assertEquals("thrift", mode);
    }

    @Test
    public void unknownReadModeIsRejected() throws Exception {
        DorisConfig config = createConfig("unknown", null);

        try {
            DorisReadModeResolver.resolve(config, () -> 9040);
            Assert.fail("Expected unknown read mode to be rejected");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Unknown read mode: unknown", e.getMessage());
        }
    }

    private DorisConfig createConfig(String readMode, Integer flightSqlPort) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("doris.fenodes", "127.0.0.1:8030");
        options.put("doris.table.identifier", "db.tbl");
        options.put("doris.user", "root");
        options.put("doris.password", "");
        if (readMode != null) {
            options.put("doris.read.mode", readMode);
        }
        if (flightSqlPort != null) {
            options.put("doris.read.arrow-flight-sql.port", flightSqlPort.toString());
        }
        return DorisConfig.fromMap(options, false);
    }
}
