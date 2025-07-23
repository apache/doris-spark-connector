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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);

    public static boolean tryHttpConnection(String host) {
        try {
            LOG.debug("try to connect host {}", host);
            host = "http://" + host;
            URL url = new URL(host);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(60000);
            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();
            connection.disconnect();
            if (responseCode < 500) {
                // code greater than 500 means a server-side exception.
                return true;
            }
            LOG.warn(
                    "Failed to connect host {}, responseCode={}, msg={}",
                    host,
                    responseCode,
                    responseMessage);
            return false;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to host:{}, cause {}", host, ex.getMessage());
            return false;
        }
    }
}
