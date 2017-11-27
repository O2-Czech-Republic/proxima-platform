#!/bin/bash
#
# Copyright 2017 O2 Czech Republic, a.s.
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

# Run the required services locally

export BIN_DIR=$(dirname $0)

source ${BIN_DIR}/config.sh

PWD=`pwd`

echo "Starting required services in docker."
echo "===================================="
echo "Stop command: docker-compose stop"
echo "Clean command: docker-compose rm"

if [ -z `which docker-compose` ]; then
  echo "Need to install docker compose (i.e apt-get install docker-compose)"
  exit 1
fi

cd $BIN_DIR
docker-compose up -d 
cd $PWD

