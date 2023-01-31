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

import os
import sys
import grpc_tools
import grpc_tools.protoc as protoc
from setuptools import setup

def get_version():
  return "0.11.0"

def build_ingest_rpc(name):
  
  proto_dir = os.path.split(os.path.abspath(sys.argv[0]))[0] + "/direct/rpc/src/main/proto/"
  protos = filter(lambda x: x.endswith(".proto"), os.listdir(proto_dir))
  protos = list(map(lambda x: proto_dir + "/" + x, protos))

  packages = []
  imports = ""
 
  os.makedirs("./target", exist_ok=True)
    
  
  args = [
    "grpc_tools.protoc",
    "--proto_path=%s" % (proto_dir, )]
  
  for item in grpc_tools.__spec__.submodule_search_locations:
    args.append("--proto_path=%s" % (item + "/_proto"))
  
  args += ["--python_out=./target", "--grpc_python_out=./target"] + protos
  
  protoc.main(args)

  for proto in protos:
    proto_name = proto.split("/")[-1].split(".")[0]
    packages += [proto_name + "_pb2", proto_name + "_pb2_grpc"]

  return packages

  
to_build = {"ingest_grpc": build_ingest_rpc}
modules = []

for name, build in to_build.items():
  result = build(name)
  modules += result

setup(name = 'proxima',
  version = get_version(),
  description = 'Proxima python bindings',
  package_dir = {'': "target"},
  py_modules = modules)
