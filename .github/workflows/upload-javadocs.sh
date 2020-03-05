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


set -eu

source $(dirname $0)/functions.sh

if [ "$(is_datadriven_repo ${GITHUB_REPOSITORY})" == "1" ]; then
  VERSION=$(proxima_version)

  mvn install -Pallow-snapshots -DskipTests && mvn site -Psite

  echo ${GOOGLE_CREDENTIALS} > /tmp/google-credentials.json
  gcloud auth activate-service-account --key-file /tmp/google-credentials.json

  gsutil -m cp -r target/site/apidocs gs://${PROXIMA_DOC_GC_STORAGE}/${VERSION}/
fi
