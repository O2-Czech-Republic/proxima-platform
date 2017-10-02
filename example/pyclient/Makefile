#
# @author: Tomas Hegr, tomas.hegr@o2.cz
# This makefile helps to generate required platform packages for the example python client.
# 1. target model: requires protoc binary to be installed. 
# 2. target rpc: requires python package grpc_tool to be installed in the current environment (part of the requirements.txt).
# In case all requirements are satisfied just call make.
#

PROTOC := $(shell command -v protoc 2 > /dev/null)
PYTHON := $(shell command -v python 2 > /dev/null)
PIP := $(shell command -v pip 2 > /dev/null)
GRPC := $(shell pip show grpcio_tools 1 > /dev/null; echo $$?)

all: model rpc

model:
ifdef PROTOC
	$(error "protoc binary is not available, please, install it.")
else
	$(info "protoc binary found.") 
	protoc -I=../proto/src/main/proto/ --python_out=./ ../proto/src/main/proto/*
endif

rpc:
ifdef PYTHON 
	$(error "python binary is not available, please, install it.")
else
	$(info "python binary found")
ifdef PIP
	$(error "pip binary is not available, please, install it.")
else
	$(info "pip binary found")
ifeq ($(GRPC),0)
	$(info "grpcio_tools package found")
	python -m grpc_tools.protoc -I ../../rpc/src/main/proto --python_out=. --grpc_python_out=. ../../rpc/src/main/proto/rpc.proto
else
	$(error "GRPCIO $(GRPC) grpcio_tools package is not available, please, install it.")
endif
endif
endif

