#!/bin/bash

if [ -z ${SPARK_LOAD_HOME} ]; then
  cur_dir=$(dirname "$0")/../
  SPARK_LOAD_HOME=$(pwd ${cur_dir})
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
    if [[ "${f}" == "spark-load-core"*".jar" ]]; then
        SPARK_LOAD_CORE_JAR="${f}"
        continue
    fi
    CLASSPATH="${f}:${CLASSPATH}"
done
CLASSPATH="${SPARK_LOAD_CORE_JAR}:${CLASSPATH}"
export CLASSPATH="${SPARK_LOAD_CORE_JAR}/conf:${CLASSPATH}:${SPARK_LOAD_CORE_JAR}/lib"

${JAVA} org.apache.doris.SparkLoadRunner "$@"