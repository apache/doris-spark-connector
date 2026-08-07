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

package org.apache.doris.spark.client.write;

import com.sun.net.httpserver.HttpServer;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.testutil.HttpsTestServer;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class AbstractCopyIntoProcessorTest {

    @Test
    public void copyUsesTlsForDorisAndASeparateClientForStorage() throws Exception {
        AtomicInteger storageUploads = new AtomicInteger();
        HttpServer storageServer =
                HttpServer.create(
                        new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        storageServer.createContext(
                "/",
                exchange -> {
                    byte[] buffer = new byte[1024];
                    while (exchange.getRequestBody().read(buffer) >= 0) {
                        // Drain the uploaded stream.
                    }
                    storageUploads.incrementAndGet();
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().close();
                    exchange.close();
                });
        storageServer.start();

        try (HttpsTestServer dorisServer = new HttpsTestServer()) {
            dorisServer.redirectTo(
                    "http://127.0.0.1:" + storageServer.getAddress().getPort() + "/upload");
            DorisConfig config =
                    HttpsTestServer.tlsConfig(
                            dorisServer.getEndpoint("localhost"), "/tls/ca.pem", false);
            TestCopyIntoProcessor processor = new TestCopyIntoProcessor(config);
            try {
                processor.load("row");
                Assert.assertNotNull(processor.stop());
                Assert.assertEquals(1, storageUploads.get());
            } finally {
                processor.close();
            }
        } finally {
            storageServer.stop(0);
        }
    }

    private static final class TestCopyIntoProcessor
            extends AbstractCopyIntoProcessor<String> {

        private TestCopyIntoProcessor(DorisConfig config) throws Exception {
            super(config);
        }

        @Override
        protected String toFormat(String row, String format) {
            return row;
        }
    }
}
