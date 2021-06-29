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


function get_name {
  POM=$1
  FILE=/tmp/VERSION.$RANDOM
  mvn help:evaluate -Dexpression=project.artifactId -q -Doutput=/$FILE -f $POM
  RET=":$(cat $FILE)"
  rm $FILE
  echo $RET
}

PROJECTS=""
SONAR=0
if [[ $1 == "sonar" ]]; then
  SONAR=1
  shift
fi

BRANCH=$1
PULL_REQUEST=$([[ $2 == "false" ]] && echo 0 || echo 1)

if [[ $BRANCH == "master" ]] && [[ $PULL_REQUEST == 0 ]]; then
  exit 0;
fi

if git log HEAD~1..HEAD | grep rebuild 1> /dev/null 2> /dev/null; then
  exit 0;
fi

for pom in $(git log --name-only origin/master..HEAD | sed "s/src\/.\+/pom.xml/" | grep pom.xml | sort | uniq); do
  if [[ ! -z $PROJECTS ]]; then
    PROJECTS=",${PROJECTS}"
  fi
  PROJECTS=$(get_name $pom)${PROJECTS}
done

if [[ $SONAR == 1 ]]; then
  if [[ ! -z ${PROJECTS} ]]; then
    echo -pl :platform-parent,${PROJECTS} -am
  else
    echo -pl :platform-parent
  fi
elif [[ ! -z ${PROJECTS} ]]; then
  echo -pl $PROJECTS -amd -am
else
  echo -N
fi
