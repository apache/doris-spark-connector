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

package org.apache.doris.spark.config;

import org.apache.doris.spark.exception.OptionRequiredException;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** TLS options shared by all Doris client protocols. */
public final class DorisTlsOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean enabled;
    private final String caCertificatePath;
    private final boolean skipHostnameVerification;
    private final Set<Protocol> excludedProtocols;

    private DorisTlsOptions(
            boolean enabled,
            String caCertificatePath,
            boolean skipHostnameVerification,
            Set<Protocol> excludedProtocols) {
        this.enabled = enabled;
        this.caCertificatePath = caCertificatePath;
        this.skipHostnameVerification = skipHostnameVerification;
        this.excludedProtocols = Collections.unmodifiableSet(EnumSet.copyOf(excludedProtocols));
    }

    public static DorisTlsOptions fromConfig(DorisConfig config) throws OptionRequiredException {
        return new DorisTlsOptions(
                config.getValue(DorisOptions.DORIS_ENABLE_TLS),
                config.getValue(DorisOptions.DORIS_TLS_CA_CERTIFICATE_PATH),
                config.getValue(DorisOptions.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION),
                parseExcludedProtocols(config.getValue(DorisOptions.DORIS_TLS_EXCLUDED_PROTOCOLS)));
    }

    private static EnumSet<Protocol> parseExcludedProtocols(String configuredProtocols) {
        EnumSet<Protocol> protocols = EnumSet.noneOf(Protocol.class);
        if (configuredProtocols == null || configuredProtocols.trim().isEmpty()) {
            return protocols;
        }
        for (String value : configuredProtocols.split(",", -1)) {
            protocols.add(Protocol.fromOptionValue(value));
        }
        return protocols;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isEnabledFor(Protocol protocol) {
        return enabled && !excludedProtocols.contains(protocol);
    }

    public String getCaCertificatePath() {
        return caCertificatePath;
    }

    public boolean isSkipHostnameVerification() {
        return skipHostnameVerification;
    }

    public Set<Protocol> getExcludedProtocols() {
        return excludedProtocols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisTlsOptions that = (DorisTlsOptions) o;
        return enabled == that.enabled
                && skipHostnameVerification == that.skipHostnameVerification
                && Objects.equals(caCertificatePath, that.caCertificatePath)
                && Objects.equals(excludedProtocols, that.excludedProtocols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                enabled,
                caCertificatePath,
                skipHostnameVerification,
                excludedProtocols);
    }

    /** Doris protocols that can be independently excluded from TLS. */
    public enum Protocol {
        HTTP("http"),
        MYSQL("mysql"),
        THRIFT("thrift"),
        ARROW_FLIGHT("arrowflight");

        private final String optionValue;

        Protocol(String optionValue) {
            this.optionValue = optionValue;
        }

        private static Protocol fromOptionValue(String value) {
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            for (Protocol protocol : values()) {
                if (protocol.optionValue.equals(normalized)) {
                    return protocol;
                }
            }
            throw new java.lang.IllegalArgumentException(
                    "Unknown TLS excluded protocol: " + value);
        }
    }
}
