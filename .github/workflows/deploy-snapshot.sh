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

function deploy() {

  VERSION=$1
  TRY=0

  echo "Going to deploy version ${VERSION}"

  mvn versions:set -DnewVersion=${VERSION}

  while [ $TRY -lt 3 ]; do
    CMD="mvn -s /tmp/settings.xml deploy -DskipTests -Prelease-snapshot -Pallow-snapshots"
    if [ ! -z "${RESUME}" ]; then
      CMD="${CMD} $(echo "${RESUME}" | sed "s/.\+\(-rf .\+\)/\1/")"
    fi
    echo "Starting to deploy step $((TRY + 1)) with command ${CMD}"
    touch output${TRY}.log
    tail -f output${TRY}.log &
    RESUME=$(${CMD} | tee output${TRY}.log | grep -A1 "After correcting the problems, you can resume the build with the command" | tail -1)
    if [ -z "${RESUME}" ]; then
      break
    fi
    TRY="$((TRY+1))"
  done
  if [ $TRY -lt 3 ]; then
    echo "Success deploying snapshot"
  else
    echo "Failed to deploy snapshot"
    return 1
  fi

}

function verify_jdk() {
  JDK=$1

  if ! mvn -version | grep "Java version: ${JDK}"; then
    echo "Failed to verify required JDK version ${JDK}"
    return 1
  fi
}

set -eu

VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep SNAPSHOT | grep -v INFO)
JDK8_VERSION=$(echo "${VERSION}" | sed "s/\(.\+\)-SNAPSHOT/\1-jdk8-SNAPSHOT/")

if [ -z "${VERSION}" ]; then
  echo "Failed to retrieve version from repository"
  exit 1
fi

TARGET_JDK=$([ $# -gt 0 ] && echo "${1}" || echo "")

if [ -z "${TARGET_JDK}" ] || [ "${TARGET_JDK}" != "8" -a "${TARGET_JDK}" != "11" ]; then
  echo "Missing target JDK argument, must be either 8 or 11"
  exit 1
fi


echo "${MAVEN_SETTINGS}" > /tmp/settings.xml
echo "${GOOGLE_CREDENTIALS}" > /tmp/google-credentials.json

export GOOGLE_APPLICATION_CREDENTIALS=/tmp/google-credentials.json

RESUME=""
if echo "${VERSION}" | grep SNAPSHOT >/dev/null && echo "${GITHUB_REPOSITORY}" | grep O2-Czech-Republic >/dev/null; then

  case "${TARGET_JDK}" in

    "8")
      verify_jdk "1\.8"
      deploy ${JDK8_VERSION}
      ;;

    "11")
      verify_jdk "11"
      deploy ${VERSION}
      ;;

  esac

fi


