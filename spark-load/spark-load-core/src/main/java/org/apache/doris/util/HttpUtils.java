package org.apache.doris.util;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HttpUtils {

    public static final int DEFAULT_CONN_TIMEOUT = 60 * 1000;
    public static final int DEFAULT_SO_TIMEOUT = 60 * 1000;

    public static CloseableHttpClient getClient() {
        return getClient(DEFAULT_CONN_TIMEOUT, DEFAULT_SO_TIMEOUT);
    }

    public static CloseableHttpClient getClient(int connectionTimeout, int socketTimeout) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout)
                .build();
        return HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
    }

    public static String getEntityContent(HttpEntity entity) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = entity.getContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

}
