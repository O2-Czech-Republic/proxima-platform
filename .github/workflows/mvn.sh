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

if [[ $1 == "8" ]]; then
  VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep SNAPSHOT | grep -v INFO)
  JDK8_VERSION=$(echo "${VERSION}" | sed "s/\(.\+\)-SNAPSHOT/\1-jdk8-SNAPSHOT/")
  mvn versions:set -DnewVersion="${JDK8_VERSION}"
fi

IS_PR=$([[ ! -z $GITHUB_HEAD_REF ]] && echo ${GITHUB_HEAD_REF} || echo false)
BRANCH=${GITHUB_REF##*/}

MVN_OPTS=$(.github/mvn-build-changed-modules.sh ${BRANCH} ${IS_PR})
echo "${BRANCH} ${IS_PR} ${MVN_OPTS}"

mvn spotless:check -B -V && mvn install -B -V -Pallow-snapshots,with-coverage,ci -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=error -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 ${MVN_OPTS}  || (sleep 5; exit 1)


if [[ $1 != "8" ]]; then
  if [ "${IS_PR}" != "false" ] || [ "${BRANCH}" == "master" ]; then
    mvn org.sonarsource.scanner.maven:sonar-maven-plugin:sonar -B -V -Pallow-snapshots,with-coverage,ci $(.github/mvn-build-changed-modules.sh sonar ${BRANCH} ${IS_PR});
  fi
fi

