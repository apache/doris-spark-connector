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
# This script is used to compile Spark-Doris-Connector
# Usage:
#    sh build.sh
#
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(cd "$ROOT"; pwd)

export DORIS_HOME=${ROOT}/../

usage() {
  echo "
  Usage:
    $0 --spark version --scala version # specify spark and scala version
    $0 --tag                           # this is a build from tag
    $0 --mvn-args -Dxx=yy -Pxx         # specify maven arguments 
  e.g.:
    $0 --spark 2.3.4 --scala 2.11
    $0 --spark 3.1.2 --scala 2.12
    $0 --tag
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'spark:' \
  -l 'scala:' \
  -l 'mvn-args:' \
  -l 'tag' \
  -- "$@")

if [ $# == 0 ] ; then
    usage
fi

eval set -- "$OPTS"

. "${DORIS_HOME}"/env.sh

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . "${DORIS_HOME}"/custom_env.sh
fi

BUILD_FROM_TAG=0
SPARK_VERSION=0
SCALA_VERSION=0
MVN_ARGS=""
while true; do
    case "$1" in
        --spark) SPARK_VERSION=$2 ; shift 2 ;;
        --scala) SCALA_VERSION=$2 ; shift 2 ;;
        --mvn-args) MVN_ARGS=$2 ; shift 2 ;;
        --tag) BUILD_FROM_TAG=1 ; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

# extract minor version:
# eg: 3.1.2 -> 3
SPARK_MINOR_VERSION=0
if [ ${SPARK_VERSION} != 0 ]; then
    SPARK_MINOR_VERSION=${SPARK_VERSION%.*}
    echo "SPARK_MINOR_VERSION: ${SPARK_MINOR_VERSION}"
fi

if [[ ${BUILD_FROM_TAG} -eq 1 ]]; then
    rm -rf ${ROOT}/output/
    ${MVN_BIN} clean package
else
    rm -rf ${ROOT}/output/
    ${MVN_BIN} clean package -Dspark.version=${SPARK_VERSION} -Dscala.version=${SCALA_VERSION} -Dspark.minor.version=${SPARK_MINOR_VERSION} $MVN_ARGS
fi

mkdir ${ROOT}/output/
cp ${ROOT}/target/spark-doris-*.jar ${ROOT}/output/

echo "*****************************************"
echo "Successfully build Spark-Doris-Connector"
echo "*****************************************"

exit 0
