ifndef GOPATH
$(error GOPATH must be defined)
endif

# When building in the context of the conda-recipe,
# use the "host" PREFIX, (not the "build" env prefix),
# which is set by conda-build.
ifdef CONDA_BUILD
    CONDA_PREFIX = ${PREFIX}
endif

ifndef CONDA_PREFIX
    define ERRMSG
    
    
    ERROR: Dvid requires an active conda environment, with dependencies already installed.
           See GUIDE.md for details. Here's the gist of it:
    
        $$ conda create -n dvid-devel && conda activate dvid-devel
        $$ ./scripts/install-developer-dependencies.sh
    
    
    endef

    $(error ${ERRMSG} )
endif

CONDA_BASE = $(shell conda info --base)

ifndef DVID_BACKENDS
    DVID_BACKENDS = badger basholeveldb filestore gbucket swift ngprecomputed
    $(info Backend not specified. Using default value: DVID_BACKENDS="${DVID_BACKENDS}")
endif

DVID_TAGS = ${DVID_BACKENDS}
ifdef DVID_LOW_MEMORY
	DVID_TAGS += lowmem
endif

export CGO_CFLAGS = -I${CONDA_PREFIX}/include
export CGO_LDFLAGS = -L${CONDA_PREFIX}/lib -Wl,-rpath,${CONDA_PREFIX}/lib


# In a Makefile, the first listed target is
# the default target for a bare 'make' command:
all: dvid dvid-backup dvid-transfer

# Executable targets
dvid: bin/dvid
dvid-backup: bin/dvid-backup
dvid-transfer: bin/dvid-transfer

# Install: Copy all executables to the CONDA_PREFIX
install: dvid dvid-backup dvid-transfer
	cp bin/dvid ${CONDA_PREFIX}/bin/dvid
	cp bin/dvid-backup ${CONDA_PREFIX}/bin/dvid-backup
	cp bin/dvid-transfer ${CONDA_PREFIX}/bin/dvid-transfer

# Compile a helper program that generates version.go
bin/dvid-gen-version: cmd/gen-version/main.go
	go build -o bin/dvid-gen-version -v -tags "${DVID_TAGS}" cmd/gen-version/main.go

# This actually runs the above program; generates version.go
# The python script here doesn't print anything, but it (potentially) overwrites
# .last-build-git-description to force a re-build of server/version.go
# if the git SHA has changed since the last build
server/version.go: bin/dvid-gen-version \
				   $(shell "${CONDA_BASE}/bin/python" scripts/record-git-description.py . .last-build-git-description)
	bin/dvid-gen-version -o server/version.go

# FIXME: This finds ALL go source files, not just the selection of sources that are needed for dvid.
DVID_SOURCES = $(shell find . -name "*.go")

bin/dvid: cmd/dvid/main.go server/version.go .last-build-git-description ${DVID_SOURCES}
	go build -o bin/dvid -v -tags "${DVID_TAGS}" cmd/dvid/main.go

bin/dvid-backup: cmd/backup/main.go
	go build -o bin/dvid-backup -v -tags "${DVID_TAGS}" cmd/backup/main.go

bin/dvid-transfer: $(shell find cmd/transfer -name "*.go")
	go build -o bin/dvid-transfer -v -tags "${DVID_TAGS}" cmd/transfer/*.go

##
## TEST
##
DVID_GO = github.com/janelia-flyem/dvid
DVID_PACKAGES = ${DVID_GO}/dvid/... ${DVID_GO}/storage/... ${DVID_GO}/datastore ${DVID_GO}/server ${DVID_GO}/datatype/... ${DVID_GO}/tests_integration

ifdef PACKAGES
	DVID_PACKAGES = ${DVID_GO}/${PACKAGES}
endif

ifdef TEST
	SPECIFIC_TEST = -test.run ${TEST}
endif

test: dvid 
	go test ${SPECIFIC_TEST} -tags "${DVID_TAGS}" ${DVID_PACKAGES}

test-verbose: dvid
	go test -v ${SPECIFIC_TEST} -tags "${DVID_TAGS}" ${DVID_PACKAGES}

# Coverage (does this repeat the test step above?)
coverage: dvid
	go test -tags "${DVID_TAGS}" -cover ${DVID_PACKAGES}

# Bench
bench:
	go test -v -tags "${DVID_TAGS}" -bench . ${DVID_PACKAGES}
	# go test -p ${GOMAXPROCS} -bench -i -tags "${DVID_BACKEND}" test dvid datastore

.PHONY: clean
clean:
	rm -f bin/*
	rm -f server/version.go
	rm -f .last-build-git-description
