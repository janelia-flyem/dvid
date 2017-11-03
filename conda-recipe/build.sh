#!/bin/bash

# GOPATH is just the build 'work' dir 
export GOPATH=$(pwd)

# The dvid repo was cloned to the appropriate internal directory
DVID_REPO=${GOPATH}/src/github.com/janelia-flyem/dvid
cd ${DVID_REPO}

##
## Update dependencies -- is this necessary?
##
${RECIPE_DIR}/go-get-dependencies.sh

DVID_BACKEND="basholeveldb gbucket"

export CGO_CFLAGS="-I${PREFIX}/include"
export CGO_LDFLAGS="-L${PREFIX}/lib"

# Build nrsc -- is it unused?
#cd ${GOPATH}/src/github.com/janelia-flyem/go/nrsc/nrsc
#go build -o ${PREFIX}/nrsc
#cd -

set -x

# dvid-gen-version
go build -o ${PREFIX}/bin/dvid-gen-version -v -tags "${DVID_BACKEND}" cmd/gen-version/main.go 

# Build DVID
${PREFIX}/bin/dvid-gen-version -o ${DVID_REPO}/server/version.go
go build -o ${PREFIX}/bin/dvid -v -tags "${DVID_BACKEND}" ${DVID_REPO}/cmd/dvid/main.go

# dvid-backup
go build -o ${PREFIX}/bin/dvid-backup -v -tags "${DVID_BACKEND}" cmd/backup/main.go 

# dvid-transfer
go build -o ${PREFIX}/bin/dvid-transfer -v -tags "${DVID_BACKEND}" cmd/transfer/*.go

set +x

# Early exit if skipping tests
if [[ "$DVID_SKIP_TESTS" != "" ]] && [[ "$DVID_SKIP_TESTS" != "0" ]]; then
    exit 0
fi

##
## TEST
##
DVID_GO="github.com/janelia-flyem/dvid"
DVID_PACKAGES=""
DVID_PACKAGES="${DVID_PACKAGES} ${DVID_GO}/dvid"
DVID_PACKAGES="${DVID_PACKAGES} ${DVID_GO}/storage/..."
DVID_PACKAGES="${DVID_PACKAGES} ${DVID_GO}/datastore"
DVID_PACKAGES="${DVID_PACKAGES} ${DVID_GO}/server"
DVID_PACKAGES="${DVID_PACKAGES} ${DVID_GO}/datatype/..."

set -x

## FIXME: This fails?
##DVID_PACKAGES="${DVID_PACKAGES} ${DVID_GO}/tests_integration"

# (verbose)
go test -v -tags "${DVID_BACKEND}" ${DVID_PACKAGES}

# For some reason labelvol had its own target in the old CMakelists...
go test -v -tags "${DVID_BACKEND}" ${DVID_GO}/datatype/labelvol

# Coverage (does this repeat the test step above?)
go test -cover -tags "${DVID_BACKEND}" ${DVID_PACKAGES}

# Bench
go test -bench -tags "${DVID_BACKEND}" ${DVID_PACKAGES}
go test -bench -i -tags "${DVID_BACKEND}" ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore

set +x
