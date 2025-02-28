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
# This script is deploy stage jars to repository.apache.org
##############################################################

MVN=${MVN:-mvn}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE.txt ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd ${PROJECT_ROOT}/spark-doris-connector

echo "Deploying to repository.apache.org"

echo "Deploying spark2.4..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-2.4_2.11 -pl spark-doris-connector-spark-2 -am

echo "Deploying spark3.1..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-3.1 -pl spark-doris-connector-spark-3.1 -am

echo "Deploying spark3.2..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-3.2 -pl spark-doris-connector-spark-3.2 -am

echo "Deploying spark3.3..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-3.3 -pl spark-doris-connector-spark-3.3 -am

echo "Deploying spark3.4..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-3.4 -pl spark-doris-connector-spark-3.4 -am

echo "Deploying spark3.5..."
${MVN} clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Pspark-3.5 -pl spark-doris-connector-spark-3.5 -am

echo "Deploy jar finished."
cd ${CURR_DIR}