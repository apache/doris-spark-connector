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

package org.apache.doris.spark.client;

import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.util.LoadBalanceList;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

public class DorisBackendHttpClient implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DorisBackendHttpClient.class);

    private final LoadBalanceList<Backend> backends;

    private transient CloseableHttpClient httpClient;

    public DorisBackendHttpClient(List<Backend> backends) {
        this.backends = new LoadBalanceList<>(backends);
    }

    public <T> T executeReq(BiFunction<Backend, CloseableHttpClient, T> reqFunc) throws Exception {
        if (httpClient == null) {
            httpClient = HttpClients.createDefault();
        }
        Exception ex = null;
        for (Backend backend : backends) {
            try {
                return reqFunc.apply(backend, httpClient);
            } catch (Exception e) {
                log.warn("Failed to execute request on backend: {}:{}", backend.getHost(), backend.getHttpPort(), e);
				backends.reportFailed(backend);
                ex = e;
            }
        }
        throw ex;
    }

    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                log.warn("Failed to close http client", e);
            }
        }
    }

}
