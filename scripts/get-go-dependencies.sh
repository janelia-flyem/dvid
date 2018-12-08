#!/bin/bash
##
## Original CMakelists.txt used a GO_GET variable.
## Is it needed here?
##

##
## All of these 'go get' commands should be fast, becuase we already pre-fetched the sources.
## See meta.yaml
##

set -e

if [[ "${GOPATH}" == "" ]]; then
  1>&2 echo "You must define GOPATH to use this script!"
  exit 1
fi

if [ ! -z "${CONDA_BUILD}" ]; then
    CONDA_PREFIX="${PREFIX}"
fi

if [ -z "${CONDA_PREFIX}" ]; then
     1>&2 echo "A conda environment must be active"
     exit 1
fi

if [[ "${CONDA_PREFIX}" == "$(conda info --base)" ]]; then
     1>&2 echo "The root conda environment is currently active.  Please use a non-root environment."
     exit 1
fi


if [[ "$(uname)" == "Linux" ]]; then
    if [[ -z "${CC}" ]]; then
        1>&2 echo "You must have a compiler installed, and CC must be defined."
        1>&2 echo "This will be done for you if you run scripts/install-compiled-dependencies.sh"
        exit 1
    fi
    ln -sf ${CC} ${CONDA_PREFIX}/bin/gcc
fi

echo "Fetching third-party go sources..."

# gopackages
go get github.com/janelia-flyem/go
cd ${GOPATH}/src/github.com/janelia-flyem/go
git submodule init
git submodule update
cd -

# gojsonschema
go get github.com/janelia-flyem/gojsonschema

# goji
go get github.com/zenazn/goji

# msgp
#go get github.com/tinylib/msgp

go get golang.org/x/net/context

# lumberjack
go get gopkg.in/natefinch/lumberjack.v2

# snappy
go get github.com/golang/snappy

# groupcache
go get github.com/golang/groupcache

# oauth2
go get golang.org/x/oauth2
go get cloud.google.com/go/compute/metadata

# gcloud
go get cloud.google.com/go/bigtable
go get cloud.google.com/go/storage
go get google.golang.org/api/option
go get google.golang.org/grpc
go get github.com/golang/protobuf/proto
go get github.com/golang/protobuf/protoc-gen-go

# gorpc
go get github.com/valyala/gorpc

# protobuf
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/gogoproto
go get github.com/gogo/protobuf/protoc-gen-gogoslick

# gofuse
go get bazil.org/fuse

# gobolt
go get github.com/boltdb/bolt

# gomdb
go get github.com/DocSavage/gomdb

# kafka
CONFLUENTINC_DIR=${GOPATH}/src/github.com/confluentinc
KAFKA_GO_DIR=${CONFLUENTINC_DIR}/confluent-kafka-go
mkdir -p ${CONFLUENTINC_DIR}

# Can't use 'go get' directly, because that gets the newest version and we want something older.
# Instead, we clone it manually, checkout the tag we want, and then build it.
if [[ -d ${KAFKA_GO_DIR} ]]; then
    cd ${KAFKA_GO_DIR} && git fetch && cd -
else
    git clone https://github.com/confluentinc/confluent-kafka-go ${KAFKA_GO_DIR}
fi
cd ${KAFKA_GO_DIR} && git checkout v0.11.6 && cd -

if [ $(uname) == "Linux" ]; then
    # For some reason, the confluent kafka package cannot be built correctly unless you set LD_LIBRARY_PATH,
    # despite the fact that our copy of librdkafka.so does correctly provide an internal RPATH.
    # (I think the kafka build scripts are not properly calling the 'ld' command with -rpath or -rpath-link.)
    #
    # FWIW, The errors look like this:
    # 
    #   #github.com/confluentinc/confluent-kafka-go/kafka
    #   /opt/rh/devtoolset-3/root/usr/libexec/gcc/x86_64-redhat-linux/4.9.2/ld: warning: libssl.so.1.0.0, needed by /opt/conda/envs/test-dvid/lib/librdkafka.so, not found (try using -rpath or -rpath-link)
    #   /opt/rh/devtoolset-3/root/usr/libexec/gcc/x86_64-redhat-linux/4.9.2/ld: warning: liblz4.so.1, needed by /opt/conda/envs/test-dvid/lib/librdkafka.so, not found (try using -rpath or -rpath-link)
    #   /opt/rh/devtoolset-3/root/usr/libexec/gcc/x86_64-redhat-linux/4.9.2/ld: warning: libcrypto.so.1.0.0, needed by /opt/conda/envs/test-dvid/lib/librdkafka.so, not found (try using -rpath or -rpath-link)
    #   /opt/conda/envs/test-dvid/lib/librdkafka.so: undefined reference to `SHA256'
    #   /opt/conda/envs/test-dvid/lib/librdkafka.so: undefined reference to `SSL_get_error'
    #   /opt/conda/envs/test-dvid/lib/librdkafka.so: undefined reference to `PKCS12_free'
    #   ...

    # So simply define LD_LIBRARY_PATH first.
    LD_LIBRARY_PATH=${CONDA_PREFIX}/lib go build github.com/confluentinc/confluent-kafka-go/kafka
else
    go build github.com/confluentinc/confluent-kafka-go/kafka
fi

# freecache
go get github.com/coocood/freecache

# Openstack Swift
go get github.com/ncw/swift

echo "Done fetching third-party go sources."
