<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Spark Connector for Apache Doris 

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Join the Doris Community at Slack](https://img.shields.io/badge/chat-slack-brightgreen)](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)

### Spark Doris Connector

More information about compilation and usage, please visit [Spark Doris Connector](https://doris.apache.org/docs/ecosystem/spark-doris-connector)

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

### QuickStart

1. download and compile Spark Doris Connector from  https://github.com/apache/doris-spark-connector, we suggest compile Spark Doris Connector  by Doris offfcial image。

```bash
$ docker pull apache/doris:build-env-ldb-toolchain-latest
```

2. the result of compile jar is like：spark-doris-connector-3.1_2.12-1.0.0-SNAPSHOT.jar

3. download spark for https://spark.apache.org/downloads.html   .if in china there have a good choice of tencent link  https://mirrors.cloud.tencent.com/apache/spark/spark-3.1.2/

```bash
#download
wget https://mirrors.cloud.tencent.com/apache/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
#decompression
tar -xzvf spark-3.1.2-bin-hadoop3.2.tgz
```

4. config Spark environment

```shell
vim /etc/profile
export SPARK_HOME=/your_parh/spark-3.1.2-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin
source /etc/profile
```

5. copy spark-doris-connector-3.1_2.12-1.0.0-SNAPSHOT.jar to spark  jars directory。

```shell
cp /your_path/spark-doris-connector/target/spark-doris-connector-3.1_2.12-1.0.0-SNAPSHOT.jar  $SPARK_HOME/jars
```

6. created  doris database and table。

   ```sql
   create database mongo_doris;
   use mongo_doris;
   CREATE TABLE data_sync_test_simple
    (
            _id VARCHAR(32) DEFAULT '',
            id VARCHAR(32) DEFAULT '',
            user_name VARCHAR(32) DEFAULT '',
            member_list VARCHAR(32) DEFAULT ''
    )
    DUPLICATE KEY(_id)
    DISTRIBUTED BY HASH(_id) BUCKETS 10
    PROPERTIES("replication_num" = "1");
   INSERT INTO data_sync_test_simple VALUES ('1','1','alex','123');
   ```

   7. Input this coed in spark-shell.

```bash
import org.apache.doris.spark._
val dorisSparkRDD = sc.dorisRDD(
  tableIdentifier = Some("mongo_doris.data_sync_test"),
  cfg = Some(Map(
    "doris.fenodes" -> "127.0.0.1:8030",
    "doris.request.auth.user" -> "root",
    "doris.request.auth.password" -> ""
  ))
)
dorisSparkRDD.collect()
```

- mongo_doris:doris database name
- data_sync_test:doris  table mame.
- doris.fenodes:doris FE IP:http_port
- doris.request.auth.user:doris  user name.
- doris.request.auth.password:doris  password

8. if Spark is Cluster model,upload Jar to HDFS，add doris-spark-connector jar HDFS URL in  spark.yarn.jars.

```bash
spark.yarn.jars=hdfs:///spark-jars/doris-spark-connector-3.1.2-2.12-1.0.0.jar
```

Link：https://github.com/apache/doris/discussions/9486

9. in pyspark,input this code in pyspark shell command.

```bash
dorisSparkDF = spark.read.format("doris")
.option("doris.table.identifier", "mongo_doris.data_sync_test")
.option("doris.fenodes", "127.0.0.1:8030")
.option("user", "root")
.option("password", "")
.load()
# show 5 lines data 
dorisSparkDF.show(5)
```

## Report issues or submit pull request

If you find any bugs, feel free to file a [GitHub issue](https://github.com/apache/doris/issues) or fix it by submitting a [pull request](https://github.com/apache/doris/pulls).

## Contact Us

Contact us through the following mailing list.

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](https://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## Links

* Doris official site - <https://doris.apache.org>
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Slack channel - [Join the Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)
