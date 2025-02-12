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

package org.apache.doris.common;

public interface Constants {

    String HIVE_METASTORE_URIS = "hive.metastore.uris";
    String SPARK_STANDALONE_SCHEME = "spark";
    String HADOOP_AUTH_KERBEROS = "kerberos";
    String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";

    String DEFAULT_CATALOG = "internal";

    String S3_ENDPOINT = "s3.endpoint";
    String S3_REGION = "s3.region";
    String S3_ACCESS_KEY = "s3.access_key";
    String S3_SECRET_KEY = "s3.secret_key";
    String S3_TOKEN = "s3.session_token";

}
