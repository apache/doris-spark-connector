#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script deploys the Spark 4.x stage jars (JDK 17,
# Scala 2.13) to repository.apache.org.
#
# It is kept separate from deploy_staging_jars.sh (Spark 2.4 -
# 3.5, JDK 8) so the two toolchains never share a run: either
# release flow can be rerun on its own if a JDK-switching issue
# occurs. This script fails fast when JDK 17 is not in effect
# rather than silently falling back to JDK 8 and breaking the
# build.
##############################################################

MVN=${MVN:-mvn}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# fail immediately
set -o errexit
set -o nounset

# PLACEHOLDER_BODY

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE.txt ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

# Spark 4.x requires JDK 17. Prefer JAVA17_HOME when set; otherwise the current JAVA_HOME must be
# JDK 17. Fail fast on a wrong or missing toolchain instead of silently building with JDK 8.
if [ -n "${JAVA17_HOME:-}" ]; then
    export JAVA_HOME="${JAVA17_HOME}"
fi
if [ -z "${JAVA_HOME:-}" ]; then
    echo "JDK 17 is required to deploy Spark 4.x. Set JAVA17_HOME (or JAVA_HOME) to a JDK 17 home."
    exit 1
fi
JAVA_VERSION=$("${JAVA_HOME}/bin/java" -version 2>&1 | awk -F'"' '/version/ {print $2}')
case "${JAVA_VERSION}" in
    17.*|18.*|19.*|2[0-9].*)
        ;;
    *)
        echo "JDK 17 or later is required to deploy Spark 4.x, but JAVA_HOME points to Java ${JAVA_VERSION}."
        echo "Set JAVA17_HOME (or JAVA_HOME) to a JDK 17 home and rerun this script."
        exit 1
        ;;
esac

###########################

cd ${PROJECT_ROOT}/spark-doris-connector

echo "Deploying to repository.apache.org with JDK ${JAVA_VERSION} (JAVA_HOME=${JAVA_HOME})"

echo "Deploying spark4.1..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-4.1 -pl spark-doris-connector-spark-4.1 -am

echo "Deploy jar finished."
cd ${CURR_DIR}
