#
# Copyright 2017-2025 O2 Czech Republic, a.s.
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

from pathlib import Path
import sys
import grpc_tools
import grpc_tools.protoc as protoc
from setuptools import setup

def get_version() -> str:
  return "0.15.0"

def build_ingest_rpc(name: str):
  # Anchor all paths to the project root (the folder that contains this setup.py)
  PROJECT_ROOT = Path(__file__).resolve().parent
  PROTO_DIR = PROJECT_ROOT / "rpc" / "src" / "main" / "proto"
  TARGET_DIR = PROJECT_ROOT / "target"

  if not PROTO_DIR.is_dir():
    raise FileNotFoundError(
      f"Expected proto dir at {PROTO_DIR} but it does not exist. "
      "Make sure you run the build from the project root and that "
      "rpc/src/main/proto is present in the sdist."
    )

  protos = sorted(str(p) for p in PROTO_DIR.glob("*.proto"))
  if not protos:
    raise FileNotFoundError(f"No .proto files found in {PROTO_DIR}")

  TARGET_DIR.mkdir(exist_ok=True)

  # Build protoc args
  args = [
    "grpc_tools.protoc",
    f"--proto_path={PROTO_DIR}",
  ]

  # Add grpc_tools’ bundled well-known types include path
  for item in getattr(grpc_tools.__spec__, "submodule_search_locations", []) or []:
    args.append(f"--proto_path={Path(item) / '_proto'}")

  args += [
    f"--python_out={TARGET_DIR}",
    f"--grpc_python_out={TARGET_DIR}",
    *protos,
  ]

  # Run generation
  code = protoc.main(args)
  if code not in (0, None):
    raise RuntimeError(f"protoc failed with exit code {code}")

  # Return the generated module names so setuptools can package them
  modules = []
  for proto in protos:
    base = Path(proto).stem
    modules += [f"{base}_pb2", f"{base}_pb2_grpc"]
  return modules

to_build = {"ingest_grpc": build_ingest_rpc}
modules = []
for name, build in to_build.items():
  modules += build(name)

setup(
  name="proxima",
  version=get_version(),
  description="Proxima python bindings",
  package_dir={"": "target"},
  py_modules=modules,
)
