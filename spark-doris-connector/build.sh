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

# Bugzilla 37848: When no TTY is available, don't output to console
have_tty=0
# shellcheck disable=SC2006
if [[ "`tty`" != "not a tty" ]]; then
    have_tty=1
fi

# Bugzilla 37848: When no TTY is available, don't output to console
have_tty=0
# shellcheck disable=SC2006
if [[ "`tty`" != "not a tty" ]]; then
    have_tty=1
fi

 # Only use colors if connected to a terminal
if [[ ${have_tty} -eq 1 ]]; then
  PRIMARY=$(printf '\033[38;5;082m')
  RED=$(printf '\033[31m')
  GREEN=$(printf '\033[32m')
  YELLOW=$(printf '\033[33m')
  BLUE=$(printf '\033[34m')
  BOLD=$(printf '\033[1m')
  RESET=$(printf '\033[0m')
else
  PRIMARY=""
  RED=""
  GREEN=""
  YELLOW=""
  BLUE=""
  BOLD=""
  RESET=""
fi

echo_r () {
    # Color red: Error, Failed
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $RED $RESET
}

echo_g () {
    # Color green: Success
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $GREEN $RESET
}

echo_y () {
    # Color yellow: Warning
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $YELLOW $RESET
}

echo_w () {
    # Color yellow: White
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $WHITE $RESET
}

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
ROOT=$(cd "$(dirname "$PRG")" &>/dev/null && pwd)
export DORIS_HOME=$(cd "$ROOT/../" &>/dev/null && pwd)

. "${DORIS_HOME}"/env.sh

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . "${DORIS_HOME}"/custom_env.sh
fi

selectScala() {
  echo 'Spark-Doris-Connector supports Scala 2.11 and 2.12. Which version do you need ?'
  select scala in "2.11" "2.12"
  do
    case $scala in
      "2.11")
        return 1
        ;;
      "2.12")
        return 2
        ;;
      *)
        echo "invalid selected, exit.."
        exit 1
        ;;
    esac
  done
}

selectSpark() {
  echo 'Spark-Doris-Connector supports multiple versions of spark. Which version do you need ?'
  select spark in "2.4" "3.1" "3.2" "3.3" "3.4" "3.5"  "other"
  do
    case $spark in
      "2.4")
        return 1
        ;;
      "3.1")
        return 2
        ;;
      "3.2")
        return 3
        ;;
      "3.3")
        return 4
        ;;
      "3.4")
        return 5
        ;;
      "3.5")
        return 6
        ;;
      "other")
        return 7
        ;;
    esac
  done
}

SCALA_VERSION=0
selectScala
ScalaVer=$?
if [ ${ScalaVer} -eq 1 ]; then
    SCALA_VERSION="2.11.12"
elif [ ${ScalaVer} -eq 2 ]; then
    SCALA_VERSION="2.12.18"
fi


SPARK_VERSION=0
selectSpark
SparkVer=$?
if [ ${SparkVer} -eq 1 ]; then
    SPARK_VERSION="2.4.8"
elif [ ${SparkVer} -eq 2 ]; then
    SPARK_VERSION="3.1.3"
elif [ ${SparkVer} -eq 3 ]; then
    SPARK_VERSION="3.2.4"
elif [ ${SparkVer} -eq 4 ]; then
    SPARK_VERSION="3.3.4"
elif [ ${SparkVer} -eq 5 ]; then
    SPARK_VERSION="3.4.3"
elif [ ${SparkVer} -eq 6 ]; then
    SPARK_VERSION="3.5.3"
elif [ ${SparkVer} -eq 7 ]; then
    # shellcheck disable=SC2162
    read -p 'Which spark version do you need? please input
    :' ver
    SPARK_VERSION=$ver
fi

if [[ $SPARK_VERSION =~ ^3.* && $SCALA_VERSION == "2.11.12" ]]; then
  echo_r "Spark 3.x is not compatible with scala 2.11, will exit."
  exit 1
fi

# extract major version:
# eg: 3.1.2 -> 3.1
SCALA_MAJOR_VERSION=0
[ ${SCALA_VERSION} != 0 ] && SCALA_MAJOR_VERSION=${SCALA_VERSION%.*}
SPARK_MAJOR_VERSION=0
[ ${SPARK_VERSION} != 0 ] && SPARK_MAJOR_VERSION=${SPARK_VERSION%.*}

echo_g " scala version: ${SCALA_VERSION}, major version: ${SCALA_MAJOR_VERSION}"
echo_g " spark version: ${SPARK_VERSION}, major version: ${SPARK_MAJOR_VERSION}"
echo_g " build starting..."

if [[ $SPARK_VERSION =~ ^3.* ]]; then
  profile_name="spark-${SPARK_MAJOR_VERSION}"
  module_suffix=${SPARK_MAJOR_VERSION}
else
  profile_name="spark-${SPARK_MAJOR_VERSION}_${SCALA_MAJOR_VERSION}"
  module_suffix="2"
fi

${MVN_BIN} clean install -P"${profile_name}" -am "$@"

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  DIST_DIR=${DORIS_HOME}/dist
  [ ! -d "$DIST_DIR" ] && mkdir "$DIST_DIR"
  dist_jar=$(ls "${ROOT}"/spark-doris-connector-spark-"${module_suffix}"/target | grep "spark-doris-" | grep -v "sources.jar" | grep -v "original-")
  rm -rf "${DIST_DIR}"/spark-doris-connector-spark-"${module_suffix}"/"${dist_jar}"
  cp "${ROOT}"/spark-doris-connector-spark-"${module_suffix}"/target/"${dist_jar}" "$DIST_DIR"

  echo_g "*****************************************************************"
  echo_g "Successfully build Spark-Doris-Connector"
  echo_g "dist: $DIST_DIR/$dist_jar "
  echo_g "*****************************************************************"
  exit 0;
else
  echo_r "Failed build Spark-Doris-Connector"
  exit $EXIT_CODE;
fi
