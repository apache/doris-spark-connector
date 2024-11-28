package org.apache.doris.spark.client;

import org.apache.doris.spark.client.entity.Backend;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

public class DorisBackendHttpClient implements Serializable {

    private final List<Backend> backends;

    private transient CloseableHttpClient httpClient;

    public DorisBackendHttpClient(List<Backend> backends) {
        this.backends = backends;
    }

    public <T> T executeReq(BiFunction<Backend, CloseableHttpClient, T> reqFunc) throws Exception {
        if (httpClient == null) {
            httpClient = HttpClients.createDefault();
        }
        Exception ex = null;
        for (Backend backend : backends) {
            try {
                return reqFunc.apply(backend, httpClient);
            } catch (Exception e) {
                // todo
                ex = e;
            }
        }
        throw ex;
    }

    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                // todo
            }
        }
    }

}
