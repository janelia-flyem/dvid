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
set -x

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
     1>&2 echo "The base conda environment is currently active.  Please use a non-base environment."
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

export CGO_ENABLED=1
export GO111MODULE=off

echo $(type go)
echo $(go version)

ensure_checkout() {
    REMOTE=$1
    LOCAL=$2
    TAG=$3
    SHA=$4

    if [[ -d ${LOCAL} ]]; then
        cd ${LOCAL} && git fetch --tags && git checkout ${TAG} && cd -
        if [[ ! -z "${SHA}" ]]; then
            cd ${LOCAL} && git pull && git fetch --tags && git checkout ${SHA} && cd -
        fi
    else
        git clone --depth 1 --branch ${TAG} ${REMOTE} ${LOCAL}
        if [[ ! -z "${SHA}" ]]; then
            cd ${LOCAL} && git pull --unshallow && git fetch --tags && git checkout ${SHA} && cd -
        fi
    fi
}

# gopackages
go get github.com/janelia-flyem/go
cd ${GOPATH}/src/github.com/janelia-flyem/go
git submodule init
git submodule update
cd -

# gojsonschema
go get github.com/janelia-flyem/gojsonschema

# protolog -- for simple binary logging
go get github.com/janelia-flyem/protolog

# goji
go get github.com/zenazn/goji

# JWT support
go get github.com/dgrijalva/jwt-go

# Google ID OAuth2 support
go get google.golang.org/api/oauth2/v2

# CORS support
go get github.com/rs/cors

# msgp
#go get github.com/tinylib/msgp

go get golang.org/x/net/context

# lumberjack
#go get gopkg.in/natefinch/lumberjack.v2
LUMBERJACK_REPO=https://github.com/natefinch/lumberjack
LUMBERJACK_DIR=${GOPATH}/src/github.com/natefinch/lumberjack
LUMBERJACK_TAG=v2.0 # Don't change this without also changing it in meta.yaml!!
ensure_checkout ${LUMBERJACK_REPO} ${LUMBERJACK_DIR} ${LUMBERJACK_TAG}

# snappy
go get github.com/golang/snappy

# groupcache
go get github.com/golang/groupcache

# oauth2
go get golang.org/x/oauth2
go get cloud.google.com/go/compute/metadata

go get google.golang.org/api/option
go get google.golang.org/api/option/internaloption
go get google.golang.org/api/iterator
go get google.golang.org/grpc

# gcloud
go get cloud.google.com/go/bigtable
go get cloud.google.com/go/storage
go get cloud.google.com/go/firestore
go get github.com/golang/protobuf/proto
go get github.com/golang/protobuf/protoc-gen-go

# gorpc
go get github.com/valyala/gorpc

# protobuf
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/gogoproto
go get github.com/gogo/protobuf/protoc-gen-gogoslick

# gofuse
# go get bazil.org/fuse

# gobolt
# go get github.com/boltdb/bolt

# gomdb
#go get github.com/DocSavage/gomdb


# ristretto (used by badger)
#
# We can't use 'go get github.com/dgraph-io/ristretto/...' because we don't want the latest tag.
# (See badger notes below. Same reason.)
#go install -i github.com/dgraph-io/ristretto
#go get github.com/dgraph-io/ristretto
RISTRETTO_REPO=https://github.com/dgraph-io/ristretto
RISTRETTO_DIR=${GOPATH}/src/github.com/dgraph-io/ristretto
RISTRETTO_TAG=v0.0.1 # Don't change this without also changing it in meta.yaml!!
ensure_checkout ${RISTRETTO_REPO} ${RISTRETTO_DIR} ${RISTRETTO_TAG}

# badger
#
# We can't use 'go get github.com/dgraph-io/badger/...' because we don't want the latest tag.
#   Eventually we should switch to using 'go get' in 'module-aware' mode:
#   https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more
#   ...in which case we'll be able to refer to a specific tag of the main badger repo.
#go install -i github.com/dgraph-io/badger
BADGER_REPO=https://github.com/dgraph-io/badger
BADGER_DIR=${GOPATH}/src/github.com/dgraph-io/badger
BADGER_TAG=v2.0.0 # Don't change this without also changing it in meta.yaml!!
ensure_checkout ${BADGER_REPO} ${BADGER_DIR} ${BADGER_TAG}

# badger dependencies
go get github.com/DataDog/zstd

go get github.com/AndreasBriese/bbloom
go get github.com/dgryski/go-farm
go get github.com/pkg/errors
go get golang.org/x/sys/unix
go get github.com/dustin/go-humanize

go get github.com/cespare/xxhash

# freecache
go get github.com/coocood/freecache

# Openstack Swift
#go get github.com/ncw/swift
SWIFT_REPO=https://github.com/ncw/swift
SWIFT_DIR=${GOPATH}/src/github.com/ncw/swift
SWIFT_TAG=master
SWIFT_SHA=753d2090bb62619675997bd87a8e3af423a00f2d
ensure_checkout ${SWIFT_REPO} ${SWIFT_DIR} ${SWIFT_TAG} ${SWIFT_SHA}

# Go Cloud Development Kit
go get gocloud.dev
go get github.com/google/wire
go get golang.org/x/xerrors

# kafka
KAFKA_GO_REPO=https://github.com/confluentinc/confluent-kafka-go
KAFKA_GO_DIR=${GOPATH}/src/github.com/confluentinc/confluent-kafka-go
KAFKA_GO_TAG=v1.3.0
ensure_checkout ${KAFKA_GO_REPO} ${KAFKA_GO_DIR} ${KAFKA_GO_TAG}

#if [ $(uname) == "Linux" ]; then
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
    #echo "need to set LD_LIBRARY_PATH"
    #export LD_LIBRARY_PATH=${CONDA_PREFIX}/lib 
    # LD_LIBRARY_PATH=${CONDA_PREFIX}/lib go build github.com/confluentinc/confluent-kafka-go/kafka
#fi
go build github.com/confluentinc/confluent-kafka-go/kafka

echo "Done fetching third-party go sources."
