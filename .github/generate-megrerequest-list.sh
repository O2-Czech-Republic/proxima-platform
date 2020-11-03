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

#
# Generate list of merge pull request in master since last tagged version (in current branch)
#

COMMIT=$(git log --tags | head -1 | cut -d' ' -f2)
git log $COMMIT..HEAD | grep "Merge pull request" | sed "s/ \+Merge pull request #\([0-9]\+\)/ - Merge pull request \[\#\1\](https:\/\/github.com\/datadrivencz\/proxima-platform\/pull\/\1)/"
