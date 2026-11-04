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

import org.apache.doris.spark.container.instance.DorisContainer;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/** Shares a MySQL-backed JDBC Catalog across the external-catalog read tests. */
public abstract class AbstractMySqlCatalogTestBase extends AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractMySqlCatalogTestBase.class);
    private static final String DATABASE = "spark_catalog_source";
    private static final String MYSQL_PASSWORD = "catalog-test";
    private static GenericContainer<?> mysql;
    private static String catalog;
    private static boolean catalogCreated;

    @BeforeClass
    public static void startMySqlCatalog() throws Exception {
        Assume.assumeTrue("This case requires the managed Doris container",
                dorisContainerService instanceof DorisContainer);
        GenericContainer<?> doris = ((DorisContainer) dorisContainerService).getContainer();
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        catalog = "spark_mysql_catalog_" + suffix;
        String mysqlHost = "mysql-catalog-" + suffix;
        mysql = new GenericContainer<>(DockerImageName.parse("mysql:8.0.36"))
                .withNetwork(doris.getNetwork())
                .withNetworkAliases(mysqlHost)
                .withExposedPorts(3306)
                .withEnv("MYSQL_ROOT_PASSWORD", MYSQL_PASSWORD)
                .withEnv("MYSQL_ROOT_HOST", "%")
                .withEnv("MYSQL_DATABASE", DATABASE)
                .waitingFor(Wait.forLogMessage(".*ready for connections.*\\n", 2))
                .withStartupTimeout(Duration.ofMinutes(2));
        mysql.start();
        String mysqlUrl = "jdbc:mysql://" + mysql.getHost() + ":" + mysql.getMappedPort(3306)
                + "/" + DATABASE + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        try (Connection connection = DriverManager.getConnection(mysqlUrl, "root", MYSQL_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE `order` (id INT PRIMARY KEY, name VARCHAR(32), score INT)");
            statement.execute("INSERT INTO `order` VALUES (1,'alpha',10),(2,'beta',20),(3,NULL,NULL)");
            statement.execute("CREATE TABLE empty_records (id INT PRIMARY KEY, name VARCHAR(32), score INT)");
        }

        Path driverJar = Paths.get(com.mysql.cj.jdbc.Driver.class
                .getProtectionDomain().getCodeSource().getLocation().toURI());
        assertTrue("Expected mysql-connector-j JAR on the test classpath: " + driverJar,
                Files.isRegularFile(driverJar) && driverJar.toString().endsWith(".jar"));
        String driverPath = "/tmp/spark-catalog-mysql-" + suffix + ".jar";
        doris.copyFileToContainer(MountableFile.forHostPath(driverJar.toString()), driverPath);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "jdbc");
        properties.put("user", "root");
        properties.put("password", MYSQL_PASSWORD);
        properties.put("jdbc_url", "jdbc:mysql://" + mysqlHost + ":3306/" + DATABASE
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        properties.put("driver_url", "file://" + driverPath);
        properties.put("driver_class", "com.mysql.cj.jdbc.Driver");
        properties.put("only_specified_database", "true");
        executeSql("CREATE CATALOG `" + catalog + "` PROPERTIES(" + renderOptions(properties) + ")");
        catalogCreated = true;
    }

    @AfterClass
    public static void stopMySqlCatalog() throws Exception {
        try {
            if (catalogCreated) {
                executeSql("DROP CATALOG `" + catalog + "`");
            }
        } finally {
            if (mysql != null) {
                mysql.stop();
            }
        }
    }

    protected Map<String, String> connectorOptions(String table) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("doris.fenodes", getFenodes());
        options.put("doris.table.identifier", "`" + catalog + "`.`" + DATABASE + "`.`" + table + "`");
        options.put("doris.user", getDorisUsername());
        options.put("doris.password", getDorisPassword());
        return options;
    }

    protected static String renderOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .map(entry -> "'" + entry.getKey().replace("'", "''") + "'='"
                        + entry.getValue().replace("'", "''") + "'")
                .collect(Collectors.joining(","));
    }

    private static void executeSql(String sql) throws Exception {
        try (Connection connection = getDorisQueryConnection()) {
            ContainerUtils.executeSQLStatement(connection, LOG, sql);
        }
    }
}
