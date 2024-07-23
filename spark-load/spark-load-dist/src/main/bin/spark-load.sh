#!/bin/bash
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

if [ -z ${SPARK_LOAD_HOME} ]; then
  cur_dir=$(dirname "$0")/../
  SPARK_LOAD_HOME=$(readlink -f ${cur_dir})
fi

export SPARK_LOAD_HOME

if [[ -z "${JAVA_HOME}" ]]; then
    if ! command -v java &>/dev/null; then
        JAVA=""
    else
        JAVA="$(command -v java)"
    fi
else
    JAVA="${JAVA_HOME}/bin/java"
fi

if [[ ! -x "${JAVA}" ]]; then
    echo "The JAVA_HOME environment variable is not set correctly"
    echo "This environment variable is required to run this program"
    echo "Note: JAVA_HOME should point to a JDK and not a JRE"
    echo "You can set JAVA_HOME in the fe.conf configuration file"
    exit 1
fi

SPARK_LOAD_CORE_JAR=
for f in "${SPARK_LOAD_HOME}/lib"/*.jar; do
    if [[ $(basename "${f}") == "spark-load-core"*".jar" ]]; then
        SPARK_LOAD_CORE_JAR="${f}"
        continue
    fi
    CLASSPATH="${f}:${CLASSPATH}"
done
CLASSPATH="${SPARK_LOAD_CORE_JAR}:${CLASSPATH}"
export CLASSPATH="${SPARK_LOAD_CORE_JAR}/conf:${CLASSPATH}:${SPARK_LOAD_CORE_JAR}/lib"

${JAVA} org.apache.doris.SparkLoadRunner "$@"