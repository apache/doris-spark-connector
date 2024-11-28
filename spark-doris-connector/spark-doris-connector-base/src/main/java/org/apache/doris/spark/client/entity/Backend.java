package org.apache.doris.spark.client.entity;

import java.io.Serializable;
import java.util.Objects;

public class Backend implements Serializable {

    private final String host;
    private final Integer httpPort;
    private final Integer rpcPort;

    public Backend(String backendString) {
        this(backendString.split(":")[0], -1, Integer.valueOf(backendString.split(":")[1]));
    }

    public Backend(String host, Integer rpcPort) {
        this(host, -1, rpcPort);
    }

    public Backend(String host, Integer httpPort, Integer rpcPort) {
        this.host = host;
        this.httpPort = httpPort;
        this.rpcPort = rpcPort;
    }

    public String getHost() {
        return host;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public Integer getRpcPort() {
        return rpcPort;
    }

    @Override
    public String toString() {
        return "Backend{" +
                "host='" + host + '\'' +
                ", httpPort=" + httpPort +
                ", rpcPort=" + rpcPort +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Backend backend = (Backend) o;
        return Objects.equals(host, backend.host) && Objects.equals(httpPort, backend.httpPort) && Objects.equals(rpcPort, backend.rpcPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, httpPort, rpcPort);
    }

    public String hostRpcPortString() {
        return String.format("%s:%d", host, rpcPort);
    }

}
