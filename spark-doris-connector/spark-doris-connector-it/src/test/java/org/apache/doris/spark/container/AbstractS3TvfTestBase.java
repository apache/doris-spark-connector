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

package org.apache.doris.spark.container;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Shared Doris and MinIO environment for S3 TVF integration tests. */
public abstract class AbstractS3TvfTestBase extends AbstractContainerTestBase {
    protected static final String S3_BUCKET = "doris-tvf-it";
    protected static final String S3_REGION = "us-east-1";
    protected static final String S3_ACCESS_KEY = "minioadmin";
    protected static final String S3_SECRET_KEY = "minioadmin";

    private static final int MINIO_PORT = 9000;
    private static final String MINIO_IMAGE =
            "minio/minio:RELEASE.2024-10-13T13-34-11Z";

    private static GenericContainer<?> minio;
    private static S3Client s3Client;
    private static String s3Endpoint;

    @BeforeClass
    public static void startObjectStorage() throws Exception {
        minio =
                new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                        .withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY)
                        .withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_KEY)
                        .withCommand("server", "/data")
                        .withExposedPorts(MINIO_PORT)
                        .waitingFor(Wait.forHttp("/minio/health/live").forPort(MINIO_PORT));
        minio.start();

        String dockerHost = DockerClientFactory.instance().dockerHostIpAddress();
        String endpointHost = resolveEndpointHost(dockerHost);
        s3Endpoint = "http://" + endpointHost + ":" + minio.getMappedPort(MINIO_PORT);
        s3Client = createS3Client(s3Endpoint);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_BUCKET).build());
    }

    @AfterClass
    public static void stopObjectStorage() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (minio != null) {
            minio.stop();
        }
    }

    protected Map<String, String> s3TvfSinkOptions(
            String tableIdentifier, String objectPrefix, String labelPrefix, int batchSize) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("doris.fenodes", getFenodes());
        options.put("doris.query.port", Integer.toString(getQueryPort()));
        options.put("doris.table.identifier", tableIdentifier);
        options.put("user", getDorisUsername());
        options.put("password", getDorisPassword());
        options.put("doris.sink.mode", "tvf");
        options.put("doris.sink.label.prefix", labelPrefix);
        options.put("doris.sink.batch.size", Integer.toString(batchSize));
        options.put("doris.sink.s3.endpoint", s3Endpoint);
        options.put("doris.sink.s3.region", S3_REGION);
        options.put("doris.sink.s3.bucket", S3_BUCKET);
        options.put("doris.sink.s3.prefix", objectPrefix);
        options.put("doris.sink.s3.access-key", S3_ACCESS_KEY);
        options.put("doris.sink.s3.secret-key", S3_SECRET_KEY);
        options.put("doris.sink.s3.path-style-access", "true");
        return options;
    }

    protected List<String> listObjectKeys(String prefix) {
        List<String> keys = new ArrayList<>();
        s3Client
                .listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(S3_BUCKET)
                                .prefix(prefix)
                                .build())
                .contents()
                .forEach(object -> keys.add(object.key()));
        return keys;
    }

    protected static String uniqueName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private static S3Client createS3Client(String endpoint) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(S3_REGION))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(S3_ACCESS_KEY, S3_SECRET_KEY)))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(
                        S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private static String resolveEndpointHost(String dockerHost) throws Exception {
        InetAddress dockerAddress = InetAddress.getByName(dockerHost);
        if (!dockerAddress.isLoopbackAddress() && !dockerAddress.isAnyLocalAddress()) {
            return dockerHost;
        }
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                continue;
            }
            Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address
                        && !address.isLoopbackAddress()
                        && !address.isLinkLocalAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        return dockerHost;
    }
}
