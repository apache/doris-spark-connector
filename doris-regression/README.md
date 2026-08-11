# Doris Regression Cases

This standalone Maven project packages the Spark connector programs used by the Apache Doris
regression suite. It is intentionally kept outside the connector Maven reactor and pins the
connector version in `pom.xml`.

## Build

```shell
mvn clean package
```

The shaded artifact is generated at `target/spark-doris-case.jar`.

## Entry Points

| Main class | Apache Doris regression case | Case-specific arguments |
| --- | --- | --- |
| `org.apache.doris.DorisSparkReadWriterDemo` | `regression-test/suites/connector_p0/spark_connector/spark_connector_read_type.groovy` | `--doris-read-table-identifier <database.table>` and `--doris-write-table-identifier <database.table>` |
| `org.apache.doris.spark.testcase.TestStreamLoadForArrowType` | `regression-test/suites/connector_p0/spark_connector/spark_connector_arrow.groovy` | `--doris-database <database>` |

All arguments are passed as name-value pairs. The common required arguments are:

```text
--doris-fe-address <host:port>
--doris-user <user>
--doris-password <password>
```

The optional TLS arguments are shared by both entry points:

```text
--doris-enable-tls <true|false>
--doris-tls-ca-certificate-path <path>
--doris-tls-skip-hostname-verification <true|false>
--doris-tls-excluded-protocols <http,mysql,thrift,arrowflight>
```

An empty value for `--doris-tls-excluded-protocols` enables TLS for every supported protocol.
