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

package org.apache.doris.spark.util;

import org.apache.doris.spark.config.DorisTlsOptions;
import org.apache.doris.spark.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);

    public static boolean tryHttpConnection(String host) {
        return tryHttpConnection(host, null);
    }

    public static boolean tryHttpConnection(String host, DorisTlsOptions tlsOptions) {
        HttpURLConnection connection = null;
        String endpoint = host;
        try {
            LOG.debug("try to connect host {}", host);
            boolean useTls = tlsOptions != null
                    && tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP);
            endpoint = (useTls ? "https://" : "http://") + host;
            URL url = new URL(endpoint);
            connection = tlsOptions == null
                    ? (HttpURLConnection) url.openConnection()
                    : DorisHttpClientFactory.openConnection(url, tlsOptions);
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(60000);
            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();
            if (responseCode < 500) {
                // code greater than 500 means a server-side exception.
                return true;
            }
            throw new IOException(
                    String.format(
                            "Endpoint probe returned responseCode=%d, msg=%s",
                            responseCode,
                            responseMessage));
        } catch (Exception ex) {
            throw new DorisRuntimeException("Failed to connect to host " + endpoint, ex);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
