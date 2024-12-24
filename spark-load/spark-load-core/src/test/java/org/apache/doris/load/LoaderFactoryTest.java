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

package org.apache.doris.load;

import org.apache.doris.common.enums.LoadMode;
import org.apache.doris.config.JobConfig;
import org.apache.doris.load.job.Loader;
import org.apache.doris.load.job.PullLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LoaderFactoryTest {

    @Test
    void createLoader() {

        JobConfig jobConfig = new JobConfig();
        jobConfig.setLoadMode(null);
        Assertions.assertThrows(NullPointerException.class, () -> LoaderFactory.createLoader(jobConfig, false));

        jobConfig.setLoadMode(LoadMode.PUSH);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> LoaderFactory.createLoader(jobConfig, false));

        jobConfig.setLoadMode(LoadMode.PULL);
        Assertions.assertDoesNotThrow(() -> LoaderFactory.createLoader(jobConfig, false));
        Loader loader = LoaderFactory.createLoader(jobConfig, false);;
        Assertions.assertInstanceOf(PullLoader.class, loader);

    }
}