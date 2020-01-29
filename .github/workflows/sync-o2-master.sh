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

if echo $GITHUB_REPOSITORY | grep datadrivencz >/dev/null; then

  mkdir -p ~/.ssh
  echo ${O2_DEPLOY_KEY} > ~/.ssh/id_rsa
  chmod 600 ~/.ssh/id_rsa
  ssh-keygen -f ~/.ssh/id_rsa -y > ~/.ssh/id_rsa.pub

  git remote add o2 git@github.com:O2-Czech-Republic/proxima-platform.git

  git fetch --all
  git checkout -B o2-master o2/master
  git pull --ff-only origin master
  git push o2-master

fi
