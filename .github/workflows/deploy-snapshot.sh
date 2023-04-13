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

  set -eu
  git submodule update

  TRY=0

  while [ $TRY -lt 3 ]; do
    echo "Starting to deploy step $((TRY + 1))"
    if ./gradlew publish -Ppublish; then
      break;
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

source $(dirname $0)/functions.sh

set -eu

export GRADLE_USER_HOME=/tmp
echo "${GRADLE_PROPERTIES}" > /tmp/gradle.properties
VERSION=$(proxima_version)

if echo "${VERSION}" | grep SNAPSHOT >/dev/null && echo "${GITHUB_REPOSITORY}" | grep O2-Czech-Republic >/dev/null; then
  deploy
fi


