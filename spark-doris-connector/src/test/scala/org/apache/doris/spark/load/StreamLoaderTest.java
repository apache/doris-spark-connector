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

package org.apache.doris.spark.load;

import org.apache.doris.spark.cfg.ConfigurationOptions;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.spark.SparkConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StreamLoaderTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void testEnableHttpsWithoutAutoRedirect() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(ConfigurationOptions.DORIS_ENABLE_HTTPS, "true");
        sparkConf.set(ConfigurationOptions.DORIS_TABLE_IDENTIFIER, "db.table");
        sparkConf.set(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT, "false");
        new StreamLoader(new SparkSettings(sparkConf), false);
        sparkConf.set(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT, "true");
        new StreamLoader(new SparkSettings(sparkConf), false);

    }

    @Test
    public void testEnableHttpsWithAutoRedirect() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(ConfigurationOptions.DORIS_ENABLE_HTTPS, "true");
        sparkConf.set(ConfigurationOptions.DORIS_TABLE_IDENTIFIER, "db.table");
        sparkConf.set(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT, "true");
        new StreamLoader(new SparkSettings(sparkConf), false);

    }

}