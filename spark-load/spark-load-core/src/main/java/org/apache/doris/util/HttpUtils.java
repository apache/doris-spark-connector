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

package org.apache.doris.util;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HttpUtils {

    public static final int DEFAULT_CONN_TIMEOUT = 60 * 1000;
    public static final int DEFAULT_SO_TIMEOUT = 60 * 1000;

    public static CloseableHttpClient getClient() {
        return getClient(DEFAULT_CONN_TIMEOUT, DEFAULT_SO_TIMEOUT);
    }

    public static CloseableHttpClient getClient(int connectionTimeout, int socketTimeout) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout)
                .build();
        return HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
    }

    public static String getEntityContent(HttpEntity entity) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = entity.getContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

}
