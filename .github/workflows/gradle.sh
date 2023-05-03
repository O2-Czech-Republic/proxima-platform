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


set -e

IS_PR=$([[ -n "${GITHUB_HEAD_REF}" ]] && echo "${GITHUB_HEAD_REF}" || echo false)
BRANCH=${GITHUB_REF##*/}

if [[ ! -z $GOOGLE_CREDENTIALS ]]; then
  echo "${GOOGLE_CREDENTIALS}" >> /tmp/google-credentials.json
fi

if [[ "${1}" == "11" ]]; then
  if [[ "${IS_PR}" != "false" ]] || [[ "${BRANCH}" == "master" ]]; then
    ./gradlew build -x test --build-cache && ./gradlew test sonar -Pwith-coverage --no-parallel --build-cache
    exit $?
  fi
fi

./gradlew build --build-cache

