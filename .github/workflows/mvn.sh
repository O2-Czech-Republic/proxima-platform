#!/bin/bash
#
# Copyright 2017-2021 O2 Czech Republic, a.s.
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

IS_PR=$([[ ! -z $GITHUB_HEAD_REF ]] && echo ${GITHUB_HEAD_REF} || echo false)
BRANCH=${GITHUB_HEAD_REF}

echo "${BRANCH} ${IS_PR}" $(.github/mvn-build-changed-modules.sh ${BRANCH} ${IS_PR}})

mvn spotless:check -B -V && mvn install -B -V -Pallow-snapshots,with-coverage,travis -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=error $(.github/mvn-build-changed-modules.sh ${BRANCH} ${IS_PR})  || (sleep 5; exit 1)

if [[ $1 != "8" ]]; then
  if [ "${IS_PR}" != "false" ] || [ "${BRANCH}" == "master" ]; then
    mvn sonar:sonar -B -V -Pallow-snapshots,with-coverage,travis $(.github/mvn-build-changed-modules.sh sonar ${BRANCH} ${IS_PR});
  fi
fi

