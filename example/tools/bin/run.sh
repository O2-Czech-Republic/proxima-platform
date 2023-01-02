#!/bin/bash
#
# Copyright 2017-2023 O2 Czech Republic, a.s.
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


BIN_DIR=$(dirname $0)
JAVAOPTS=""
SHADED_JAR_NAME="proxima-example-tools.jar"

if [ -z "${HADOOP_HOME}"]; then
  HADOOP_HOME=$(pwd)
fi

if [ -z "${LOG_LEVEL}" ]; then
  LOG_LEVEL="INFO"
fi
JAVAOPTS="${JAVAOPTS} -DLOG_LEVEL=${LOG_LEVEL}"

if [ -n "${HEAP_SIZE}" ]; then
  JAVAOPTS="${JAVAOPTS} -Xmx${HEAP_SIZE} -Xms${HEAP_SIZE}"
fi

if [ -f "${BIN_DIR}/${SHADED_JAR_NAME}" ]; then
  JAR="${BIN_DIR}/${SHADED_JAR_NAME}"
else
  JAR="${BIN_DIR}/../target/${SHADED_JAR_NAME}"
fi

if [ -z "${CONSOLE_RUNNER}" ]; then
  CONSOLE_RUNNER=direct
fi

CLASS=${1:-"cz.o2.proxima.tools.groovy.Console"}

java -cp ${JAR} -Djava.library.path=${BIN_DIR}/hadoop-native/ ${JAVAOPTS} ${CLASS} --runner=${CONSOLE_RUNNER} \
  $(eval echo ${RUNNER_SPECIFIC_ARGS}) \
  $(eval echo $*)

