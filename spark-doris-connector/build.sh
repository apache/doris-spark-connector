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
# OS specific support.  $var _must_ be set to either true or false.
cygwin=false
os400=false
# shellcheck disable=SC2006
case "`uname`" in
CYGWIN*) cygwin=true;;
OS400*) os400=true;;
esac

# resolve links - $0 may be a softlink
PRG="$0"

while [[ -h "$PRG" ]]; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
# shellcheck disable=SC2006
ROOT=`dirname "$PRG"`
export DORIS_HOME=$(cd "$ROOT/../" &>/dev/null && pwd)

usage() {
  echo "
  Usage:
    $0 --spark version --scala version # specify spark and scala version
    $0 --tag                           # this is a build from tag
    $0 --mvn-args -Dxx=yy -Pxx         # specify maven arguments 
  e.g.:
    $0 --spark 2.3.4 --scala 2.11
    $0 --spark 3.1.2 --scala 2.12
    $0 --spark 3.2.0 --scala 2.12 --mvn-args \"-Dnetty.version=4.1.68.Final -Dfasterxml.jackson.version=2.12.3\"
    $0 --tag
  "
  exit 1
}

# we use GNU enhanced version getopt command here for long option names, rather than the original version.
# check the version of the getopt command before using.
getopt -T > /dev/null && echo "
  The GNU version of getopt command is required.
  On Mac OS, you can use Homebrew to install gnu-getopt: 
    1. brew install gnu-getopt                        # install gnu-getopt
    2. GETOPT_PATH=\`brew --prefix gnu-getopt\`         # get the gnu-getopt execute path
    3. export PATH=\"\${GETOPT_PATH}/bin:\$PATH\"         # set gnu-getopt as default getopt
" && exit 1

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
    echo "spark minor version: ${SPARK_MINOR_VERSION}"
fi

if [[ ${BUILD_FROM_TAG} -eq 1 ]]; then
    ${MVN_BIN} clean package
else
    ${MVN_BIN} clean package \
    -Dspark.version=${SPARK_VERSION} \
    -Dscala.version=${SCALA_VERSION} \
    -Dthrift.binary=${THRIFT_BIN} \
    -Dspark.minor.version=${SPARK_MINOR_VERSION} $MVN_ARGS
fi

DIST_DIR=${DORIS_HOME}/dist
[ ! -d "$DIST_DIR" ] && mkdir "$DIST_DIR"
dist_jar=$(ls "${ROOT}"/target | grep "spark-doris-" | grep -v "sources.jar" | grep -v "original-")
rm -rf "${DIST_DIR}"/"${dist_jar}"
cp "${ROOT}"/target/"${dist_jar}" "$DIST_DIR"

echo "*****************************************************************"
echo "Successfully build Spark-Doris-Connector"
echo "dist: $DIST_DIR/$dist_jar "
echo "*****************************************************************"

exit 0
