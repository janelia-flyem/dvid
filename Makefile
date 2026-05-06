# When building in the context of the conda-recipe,
# use the "host" PREFIX, (not the "build" env prefix),
# which is set by conda-build.
ifdef CONDA_BUILD
    CONDA_PREFIX = ${PREFIX}
endif

PYTHON ?= python3

DVID_DEFAULT_BACKENDS = badger filestore ngprecomputed
DVID_BASHOLEVELDB_BACKENDS = badger basholeveldb filestore ngprecomputed

ifndef DVID_BACKENDS
    DVID_BACKENDS = ${DVID_DEFAULT_BACKENDS}
endif

DVID_TAGS = ${DVID_BACKENDS}
ifdef DVID_LOW_MEMORY
    DVID_TAGS += lowmem
endif

export CGO_ENABLED = 1
export GO111MODULE = on

ifdef CONDA_PREFIX
    export CGO_CFLAGS += -I${CONDA_PREFIX}/include
    export CGO_LDFLAGS += -L${CONDA_PREFIX}/lib
    # Only add -rpath if conda's activation scripts haven't already set it in LDFLAGS.
    # The go-cgo conda package pulls in clang, whose activation scripts inject
    # -Wl,-rpath,${CONDA_PREFIX}/lib into LDFLAGS. Adding it again here causes
    # "duplicate LC_RPATH" errors/warnings from the macOS linker.
    ifeq (,$(findstring -rpath,${LDFLAGS}))
        export CGO_LDFLAGS += -Wl,-rpath,${CONDA_PREFIX}/lib
    endif
endif

ifeq ($(shell uname -s),Darwin)
    ifdef MACOSX_DEPLOYMENT_TARGET
        $(info MACOSX_DEPLOYMENT_TARGET is ${MACOSX_DEPLOYMENT_TARGET})
        export CGO_CFLAGS += -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}
        export CGO_LDFLAGS += -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}
	endif
endif

ifdef CONDA_PREFIX
    INSTALL_PREFIX ?= ${CONDA_PREFIX}
else
    INSTALL_PREFIX ?= /usr/local
endif

# In a Makefile, the first listed target is
# the default target for a bare 'make' command:
all: dvid dvid-backup dvid-transfer

# Executable targets
dvid: bin/dvid
dvid-backup: bin/dvid-backup
dvid-transfer: bin/dvid-transfer
analyze-block: bin/analyze-block
analyze-index: bin/analyze-index
body-blocks: bin/body-blocks
filter-mutations: bin/filter-mutations
compare-mappings: bin/compare-mappings
dvid-basholeveldb: DVID_BACKENDS = ${DVID_BASHOLEVELDB_BACKENDS}
dvid-basholeveldb: bin/dvid-basholeveldb

bin:
	install -d bin

# Install: Copy all executables to the install prefix.
install: dvid dvid-backup dvid-transfer analyze-block analyze-index body-blocks filter-mutations
	install -d ${INSTALL_PREFIX}/bin
	install bin/dvid ${INSTALL_PREFIX}/bin/dvid
	install bin/dvid-backup ${INSTALL_PREFIX}/bin/dvid-backup
	install bin/dvid-transfer ${INSTALL_PREFIX}/bin/dvid-transfer
	install bin/analyze-block ${INSTALL_PREFIX}/bin/analyze-block
	install bin/analyze-index ${INSTALL_PREFIX}/bin/analyze-index
	install bin/body-blocks ${INSTALL_PREFIX}/bin/body-blocks
	install bin/filter-mutations ${INSTALL_PREFIX}/bin/filter-mutations

# Compile a helper program that generates version.go
bin/dvid-gen-version: cmd/gen-version/main.go | bin
	go build -o bin/dvid-gen-version -v -tags "${DVID_TAGS}" cmd/gen-version/main.go

# This actually runs the above program; generates version.go
# The python script here doesn't print anything, but it (potentially) overwrites
# .last-build-git-description to force a re-build of server/version.go
# if the git SHA has changed since the last build
server/version.go: bin/dvid-gen-version \
				   $(shell "${PYTHON}" scripts/record-git-description.py . .last-build-git-description)
	bin/dvid-gen-version -o server/version.go

# FIXME: This finds ALL go source files, not just the selection of sources that are needed for dvid.
DVID_SOURCES = $(shell find . -name "*.go")

HEADERPATH := 
ifneq ($(OS),Windows_NT)
    ifeq ($(shell uname -s),Darwin)
        ifeq (${CONDA_BUILD_SYSROOT},)
            HEADERPATH=$(shell xcrun --sdk macosx --show-sdk-path)
		else
            HEADERPATH := ${CONDA_BUILD_SYSROOT}
        endif
    endif
endif

ifneq ($(HEADERPATH),)
    $(info HEADERPATH is $(HEADERPATH))
endif

bin/dvid: export SDKROOT=$(HEADERPATH)
bin/dvid: cmd/dvid/main.go server/version.go .last-build-git-description ${DVID_SOURCES} | bin
	go build -o bin/dvid -v -tags "${DVID_TAGS}" cmd/dvid/main.go

bin/dvid-basholeveldb: export SDKROOT=$(HEADERPATH)
bin/dvid-basholeveldb: cmd/dvid/main.go server/version.go .last-build-git-description ${DVID_SOURCES} | bin
	go build -o bin/dvid-basholeveldb -v -tags "${DVID_TAGS}" cmd/dvid/main.go

bin/dvid-backup: cmd/backup/main.go | bin
	go build -o bin/dvid-backup -v -tags "${DVID_TAGS}" cmd/backup/main.go

bin/dvid-transfer: $(shell find cmd/transfer -name "*.go") | bin
	go build -o bin/dvid-transfer -v -tags "${DVID_TAGS}" cmd/transfer/*.go

bin/analyze-block: $(shell find cmd/labelmap-utils/analyze-block -name "*.go") | bin
	go build -o bin/analyze-block -v -tags "${DVID_TAGS}" cmd/labelmap-utils/analyze-block/*.go

bin/analyze-index: $(shell find cmd/labelmap-utils/analyze-index -name "*.go") | bin
	go build -o bin/analyze-index -v -tags "${DVID_TAGS}" cmd/labelmap-utils/analyze-index/*.go

bin/body-blocks: $(shell find cmd/labelmap-utils/body-blocks -name "*.go") | bin
	go build -o bin/body-blocks -v -tags "${DVID_TAGS}" cmd/labelmap-utils/body-blocks/*.go

bin/filter-mutations: $(shell find cmd/labelmap-utils/filter-mutations -name "*.go") | bin
	go build -o bin/filter-mutations -v -tags "${DVID_TAGS}" cmd/labelmap-utils/filter-mutations/*.go

bin/compare-mappings: cmd/compare-mappings/main.go | bin
	go build -o bin/compare-mappings -v cmd/compare-mappings/main.go

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
	go tool gotestsum --format short --jsonfile test-failures.json -- ${SPECIFIC_TEST} -tags "${DVID_TAGS}" ${DVID_PACKAGES}

test-verbose: dvid
	go tool gotestsum --format standard-verbose --jsonfile test-failures.json -- ${SPECIFIC_TEST} -tags "${DVID_TAGS}" ${DVID_PACKAGES}

# Coverage (does this repeat the test step above?)
coverage: dvid
	go test -tags "${DVID_TAGS}" -cover ${DVID_PACKAGES}

# Bench
bench:
	go test -v -tags "${DVID_TAGS}" -bench . ${DVID_PACKAGES}
	# go test -p ${GOMAXPROCS} -bench -i -tags "${DVID_BACKEND}" test dvid datastore

.PHONY: dvid-basholeveldb clean
clean:
	rm -f bin/*
	rm -f server/version.go
	rm -f .last-build-git-description
