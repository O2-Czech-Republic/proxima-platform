#!/bin/bash
#
# Copyright 2017-2020 O2 Czech Republic, a.s.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

BIN_DIR=$(dirname $0)
LOG_CFG=log4j.properties
JAVAOPTS=""
SHADED_JAR_NAME="proxima-example-ingest-server.jar"

if [ -z "${HADOOP_HOME}"]; then
  HADOOP_HOME=$(pwd)
fi

if [ -z "${LOG_LEVEL}" ]; then
  LOG_LEVEL="INFO"
fi
JAVAOPTS="${JAVAOPTS} -DLOG_LEVEL=${LOG_LEVEL} -Dlog4j.configuration=${LOG_CFG}"

if [ -n "${HEAP_SIZE}" ]; then
  JAVAOPTS="${JAVAOPTS} -Xmx${HEAP_SIZE} -Xms${HEAP_SIZE}"
fi

if [ -f "${BIN_DIR}/${SHADED_JAR_NAME}" ]; then
  JAR="${BIN_DIR}/${SHADED_JAR_NAME}"
else
  JAR="${BIN_DIR}/../target/${SHADED_JAR_NAME}"
fi
CLASS=${1:-"cz.o2.proxima.server.IngestServer"}

CONFIG=""
if [ -n "${2} "]; then
  CONFIG="${2}"
fi
# Enable this for kerberized kafka
#JAVAOPTS="${JAVAOPTS} -Djava.security.auth.login.config=kafka_jaas.conf"

java -cp ${JAR} -Djava.library.path=${BIN_DIR}/hadoop-native/ ${JAVAOPTS} ${CLASS} ${CONFIG}