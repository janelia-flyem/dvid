ifndef GOPATH
$(error GOPATH must be defined)
endif


ifndef CONDA_PREFIX
define ERRMSG


ERROR: Dvid requires an active conda environment, with dependencies already installed.
       See GUIDE.md for details. Here's the gist of it:

    $$ conda create -n dvid-devel && source activate dvid-devel
    $$ ./scripts/install-developer-dependencies.sh


endef
$(error ${ERRMSG} )
endif


ifndef DVID_BACKENDS
DVID_BACKENDS = basholeveldb gbucket
endif

export CGO_CFLAGS = -I${CONDA_PREFIX}/include
export CGO_LDFLAGS = -L${CONDA_PREFIX}/lib -Wl,-rpath,${CONDA_PREFIX}/lib

dvid: bin/dvid
dvid-backup: bin/dvid-backup
dvid-transfer: bin/dvid-transfer

#FAKE_PARAM := $(shell )

# Compile the program that generates version.go
bin/dvid-gen-version: cmd/gen-version/main.go
	go build -o bin/dvid-gen-version -v -tags "${DVID_BACKENDS}" cmd/gen-version/main.go

# Actually run the above program; generates version.go
# The python script here doesn't print anything, but it (potentially) overwrites 
# .last-build-git-sha to force a re-build of server/version.go
# if the git SHA has changed since the last build
server/version.go: bin/dvid-gen-version \
				   $(shell python scripts/record-git-sha.py . .last-build-git-sha)
	bin/dvid-gen-version -o server/version.go

# FIXME: This finds ALL go source files, not just the selection of sources that are needed for dvid.
DVID_SOURCES = $(shell find . -name "*.go")

bin/dvid: cmd/dvid/main.go server/version.go .last-build-git-sha ${DVID_SOURCES}
	go build -o bin/dvid -v -tags "${DVID_BACKENDS}" cmd/dvid/main.go

bin/dvid-backup: cmd/backup/main.go
	go build -o bin/dvid-backup -v -tags "${DVID_BACKENDS}" cmd/backup/main.go 

bin/dvid-transfer: $(shell find cmd/transfer -name "*.go")
	go build -o bin/dvid-transfer -v -tags "${DVID_BACKENDS}" cmd/transfer/*.go

##
## TEST
##
DVID_GO = "github.com/janelia-flyem/dvid"
DVID_PACKAGES = ${DVID_GO}/dvid/... ${DVID_GO}/storage/... ${DVID_GO}/datastore ${DVID_GO}/server ${DVID_GO}/datatype/... ${DVID_GO}/tests_integration

test: dvid
	go test -tags "${DVID_BACKENDS}" ${DVID_PACKAGES}

test-verbose: dvid
	go test -v -tags "${DVID_BACKENDS}" ${DVID_PACKAGES}

# Coverage (does this repeat the test step above?)
coverage: dvid
	go test -cover -tags "${DVID_BACKEND}" ${DVID_PACKAGES}

# Bench
bench:
	go test -bench -tags "${DVID_BACKEND}" ${DVID_PACKAGES}
	go test -bench -i -tags "${DVID_BACKEND}" test dvid datastore

.PHONY: clean
clean:
	rm -f bin/*
	rm -f server/version.go
	rm -f .last-build-git-sha
