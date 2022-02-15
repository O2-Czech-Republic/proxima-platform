# Example python client for Ingest platform

This is an example python client for the example project of the Ingest platform in which is included. It pushes and pulls data according to the given model defined in proto folder.

## Getting Started

You should set up your environment first to be able to run the client. The client was tested with Python 3.5. It is recommended to run it in some virtual environment VirtaulEnv.

### Prerequisites

* python >= 3.7
* pip
* protoc
* python packages from [requirements.txt](requirements.txt)

```
(PXSExamplePyClient):pyclient$ python3 -m pip install -r requirements.txt
```

### Installing

To successfully run the client, you need to generate required proto packages (grpc services and models). For this reason, the repo contains Makefile that takes care of everything. If you all prerequisites are satisfied you can just run `make` command inside the client folder.

## Running the client

There is nothing easier than to run the following command in the proper environment.
```
(PXSExamplePyClient):pyclient$ python pyclient.py
```
And the platform is feed by a random data.

## Authors

* **Tomas Hegr** - *Initial work*
* **Jan Lukavsky** - *Reviewer*

