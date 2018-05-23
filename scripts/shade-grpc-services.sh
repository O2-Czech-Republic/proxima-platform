#!/usr/bin/env bash
#
# Copyright 2017-2018 O2 Czech Republic, a.s.
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

function usage {
  echo "Usage: $0 <jar> <pattern> <shade_pattern>"
}

if [ $# -lt 3 ]; then
  usage
  exit 1
fi

if [ ! -e $1 ]; then
  echo "File not found: $1"
  exit 2
fi

JAR=$(realpath $1)
PATTERN=$2
SHADED=$3

echo "Replacing META-INF/services/io.grpc.* in $JAR"

DIR=$(dirname $JAR)
TMP=${DIR}/shade-tmp

set -e

mkdir $TMP || true
cd $TMP
jar xf $JAR
cd META-INF/services
for f in $(ls -1 *); do
  sed -i.bak "s/^${PATTERN}/${SHADED}/" $f
  rm $f.bak
done
rename "s/${PATTERN}/${SHADED}/" * -v -n
cd ../../
jar cf $JAR .
cd $DIR
#rm -rf $TMP


