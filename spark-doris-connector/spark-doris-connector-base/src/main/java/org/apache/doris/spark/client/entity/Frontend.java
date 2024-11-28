package org.apache.doris.spark.client.entity;

import java.io.Serializable;

// This code is translated from Scala to Java
public class Frontend implements Serializable {

    private String host;
    private int httpPort;
    private int queryPort;
    private int flightSqlPort;

    public Frontend(String host, int httpPort) {
        this(host, httpPort, -1, -1);
    }

    public Frontend(String host, int httpPort, int queryPort) {
        this(host, httpPort, queryPort, -1);
    }

    public Frontend(String host, int httpPort, int queryPort, int flightSqlPort) {
        this.host = host;
        this.httpPort = httpPort;
        this.queryPort = queryPort;
        this.flightSqlPort = flightSqlPort;
    }

    // Getters
    public String getHost() {
        return host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getFlightSqlPort() {
        return flightSqlPort;
    }

    public String hostHttpPortString() {
        return host + ":" + httpPort;
    }

    public String hostQueryPortString() {
        return host + ":" + queryPort;
    }

}