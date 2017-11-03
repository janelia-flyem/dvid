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

if [[ "$GOPATH" == "" ]]; then
  1>&2 echo "You must define GOPATH to use this script!"
  exit 1
fi

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
