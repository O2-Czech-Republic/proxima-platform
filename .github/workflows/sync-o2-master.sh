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


set -eu

source $(dirname $0)/functions.sh

if [ "$(is_datadriven_repo ${GITHUB_REPOSITORY})" == "1" ]; then

  SSH_HOME_DIR="/tmp/.ssh"
  mkdir -p "${SSH_HOME_DIR}"
  echo "${O2_DEPLOY_KEY}" | base64 -d > "${SSH_HOME_DIR}/id_rsa_datadriven"
  chmod 600 "${SSH_HOME_DIR}/id_rsa_datadriven"
  ssh-keygen -f "${SSH_HOME_DIR}/id_rsa_datadriven" -y > "${SSH_HOME_DIR}/id_rsa_datadriven.pub"

  export GIT_SSH_COMMAND="ssh -i ${SSH_HOME_DIR}/id_rsa_datadriven"
  git remote add o2 git@github.com:O2-Czech-Republic/proxima-platform.git

  git fetch --all
  git checkout -B o2-master o2/master
  git pull --ff-only origin master
  git push o2 HEAD:master

fi
