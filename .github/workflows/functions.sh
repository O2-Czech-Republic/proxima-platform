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


proxima_version() {
  ./gradlew properties | grep "version: " | cut -c10-
}

is_snapshot_version() {
  [ -z "$1" ] && messageFail "Missing argument 1" && RET=1

  echo $1 | grep -i "SNAPSHOT" > /dev/null
  if [ $? -eq 0 ]; then
    echo 1
  else
   echo 0
  fi
}

is_datadriven_repo() {
  [ -z "$1" ] && messageFail "Missing argument 1" && RET=1

  echo $1 | grep -i "datadriven" >/dev/null
  if [ $? -eq 0 ]; then
    echo 1
  else
   echo 0
 fi
}

# ----- TEST FUNCTIONS ----
assertEquals() {
  if [ "$1" != "$2" ]; then
    messageFail "$1 not equals to $2"
    RET=1
  else
    messageOK "$1 is equals to $2"
  fi
}
assertNotEmpty() {
  [ -z "$1" ] && messageFail "Missing argument 1! Arguments: $@" && RET=1
  if [ -z $1 ]; then
    messageFail "FAIL: $1 is empty"
    RET=1
  else
   messageOK  "$1 is not empty"
  fi
}

messageOK() {
  echo "OK: $1"
}

messageFail() {
  echo "FAILED: $@" 1>&2
}
