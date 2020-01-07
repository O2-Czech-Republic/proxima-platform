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


MODULES=""
for d in beam-*; do
  sed "s/\${artifactId}/$d/" vendor-template-pom.xml > $d/pom.xml
  MODULES="${MODULES}    <module>$d<\\/module>\n"
  cd $d
  ln -s ../license-header-spotless.txt
  cd ..
done

sed "s/\${beam-vendor-modules}/${MODULES}/" pom-template.xml > pom.xml
