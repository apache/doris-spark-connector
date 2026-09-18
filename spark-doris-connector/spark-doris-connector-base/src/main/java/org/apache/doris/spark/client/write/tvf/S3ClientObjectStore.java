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

import org.apache.doris.spark.config.S3TvfOptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

/** AWS SDK based implementation for S3-compatible object storage. */
public final class S3ClientObjectStore implements S3ObjectStore {
    private static final String JSON_LINES_CONTENT_TYPE = "application/x-ndjson";
    private static final String ROLE_SESSION_NAME = "doris-spark-connector";

    private final S3Client client;
    private final String bucket;
    private DefaultCredentialsProvider defaultCredentialsProvider;
    private StsClient stsClient;
    private StsAssumeRoleCredentialsProvider assumeRoleCredentialsProvider;

    public S3ClientObjectStore(S3TvfOptions options) {
        this.bucket = options.getBucket();
        this.client = createClient(options);
    }

    S3ClientObjectStore(S3Client client, String bucket) {
        this.client = client;
        this.bucket = bucket;
    }

    private S3Client createClient(S3TvfOptions options) {
        return S3Client.builder()
                .endpointOverride(URI.create(options.getEndpoint()))
                .region(Region.of(options.getRegion()))
                .credentialsProvider(createCredentialsProvider(options))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(options.isPathStyleAccess())
                                .build())
                .build();
    }

    private AwsCredentialsProvider createCredentialsProvider(S3TvfOptions options) {
        if (!options.hasRoleArn()) {
            return staticCredentialsProvider(options);
        }
        AwsCredentialsProvider sourceCredentialsProvider;
        if (options.hasStaticCredentials()) {
            sourceCredentialsProvider = staticCredentialsProvider(options);
        } else {
            defaultCredentialsProvider = DefaultCredentialsProvider.builder().build();
            sourceCredentialsProvider = defaultCredentialsProvider;
        }
        stsClient = StsClient.builder()
                .region(Region.of(options.getRegion()))
                .credentialsProvider(sourceCredentialsProvider)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        assumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(buildAssumeRoleRequest(options))
                .build();
        return assumeRoleCredentialsProvider;
    }

    static AssumeRoleRequest buildAssumeRoleRequest(S3TvfOptions options) {
        AssumeRoleRequest.Builder request = AssumeRoleRequest.builder()
                .roleArn(options.getRoleArn())
                .roleSessionName(ROLE_SESSION_NAME);
        if (options.getExternalId() != null) {
            request.externalId(options.getExternalId());
        }
        return request.build();
    }

    private static StaticCredentialsProvider staticCredentialsProvider(S3TvfOptions options) {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(options.getAccessKey(), options.getSecretKey()));
    }

    @Override
    public void put(String objectKey, byte[] content) throws IOException {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .contentType(JSON_LINES_CONTENT_TYPE)
                .build();
        try {
            client.putObject(request, RequestBody.fromContentProvider(
                    () -> new ByteArrayInputStream(content),
                    content.length,
                    JSON_LINES_CONTENT_TYPE));
        } catch (RuntimeException e) {
            throw new IOException("Failed to upload S3 TVF object: " + objectKey, e);
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } finally {
            if (assumeRoleCredentialsProvider != null) {
                assumeRoleCredentialsProvider.close();
            }
            if (stsClient != null) {
                stsClient.close();
            }
            if (defaultCredentialsProvider != null) {
                defaultCredentialsProvider.close();
            }
        }
    }
}
