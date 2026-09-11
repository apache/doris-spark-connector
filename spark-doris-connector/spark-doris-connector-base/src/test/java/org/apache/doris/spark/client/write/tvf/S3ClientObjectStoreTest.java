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

package org.apache.doris.spark.client.write.tvf;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class S3ClientObjectStoreTest {
    @Test
    public void uploadBodyIsRepeatableWithoutCopying() throws Exception {
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        RequestBody[] captured = new RequestBody[1];
        S3Client client = new S3Client() {
            @Override
            public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
                Assert.assertEquals("bucket", request.bucket());
                Assert.assertEquals("prefix/file.json", request.key());
                Assert.assertEquals("application/x-ndjson", request.contentType());
                captured[0] = body;
                return PutObjectResponse.builder().build();
            }

            @Override
            public String serviceName() {
                return "s3";
            }

            @Override
            public void close() {}
        };
        try (S3ClientObjectStore store = new S3ClientObjectStore(client, "bucket")) {
            store.put("prefix/file.json", content);
            RequestBody body = captured[0];
            Assert.assertEquals(content.length, body.optionalContentLength().get().longValue());
            // Mutate only in this test to detect a defensive copy in the request body.
            content[0] = '[';
            try (DataInputStream first = new DataInputStream(body.contentStreamProvider().newStream());
                    DataInputStream retry = new DataInputStream(body.contentStreamProvider().newStream())) {
                Assert.assertEquals('[', first.read());
                assertContent(retry, content);
                byte[] remaining = new byte[content.length - 1];
                System.arraycopy(content, 1, remaining, 0, remaining.length);
                assertContent(first, remaining);
            }
            Assert.assertEquals("application/x-ndjson", body.contentType());
        }
    }

    private static void assertContent(DataInputStream input, byte[] expected) throws IOException {
        byte[] actual = new byte[expected.length];
        input.readFully(actual);
        Assert.assertArrayEquals(expected, actual);
        Assert.assertEquals(-1, input.read());
    }
}
