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
    INSTALL_PREFIX ?= ${HOME}/.local
endif

# In a Makefile, the first listed target is
# the default target for a bare 'make' command:
all: dvid tools

# Group: command-line utilities that ship alongside the main dvid server.
# Build with `make tools`; built binaries land in bin/ and are picked up
# by `make install`.
tools: dvid-backup dvid-transfer analyze-block analyze-index body-blocks filter-mutations

# Canonical list of binaries that `make install` will copy from bin/ to
# ${INSTALL_PREFIX}/bin if they exist. Add new top-level executables here.
# Build helpers like dvid-gen-version are intentionally omitted.
INSTALLABLE_BINS = dvid dvid-basholeveldb dvid-backup dvid-transfer \
                   analyze-block analyze-index body-blocks filter-mutations \
                   compare-mappings

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

# Builds that include the legacy basholeveldb backend require an active
# non-base conda environment so that CGO links against Basho's libleveldb
# fork (from the flyem-forge channel), not a stock libleveldb. Linking
# against upstream Google LevelDB risks corruption on legacy DVID repos.
.PHONY: require-conda-for-basholeveldb
require-conda-for-basholeveldb:
	@if [ -z "$$CONDA_PREFIX" ] || [ "$$CONDA_DEFAULT_ENV" = "base" ]; then \
		echo ""; \
		echo "ERROR: Builds with the basholeveldb backend require an active"; \
		echo "       non-base conda environment that has the flyem-forge"; \
		echo "       basholeveldb package installed."; \
		echo ""; \
		echo "Legacy DVID repositories were written by Basho's fork of"; \
		echo "LevelDB, not upstream Google LevelDB. The flyem-forge"; \
		echo "basholeveldb conda package is built from Basho's fork and is"; \
		echo "the only supported way to safely open those stores. Linking"; \
		echo "against a stock libleveldb risks corruption on legacy repos."; \
		echo ""; \
		echo "See the 'Legacy Basho LevelDB Build' section of GUIDE.md."; \
		echo ""; \
		exit 1; \
	fi

# Guard the default bin/dvid build when the user has manually added
# basholeveldb to DVID_BACKENDS. (The dvid-basholeveldb target depends
# on the guard directly, since its target-specific DVID_BACKENDS override
# does not feed back into this global findstring evaluation.)
ifneq (,$(findstring basholeveldb,${DVID_TAGS}))
    DVID_BUILD_GUARDS = require-conda-for-basholeveldb
endif

bin:
	install -d bin

# Install: copy already-built executables from bin/ to the install prefix.
# Does NOT trigger a build. Run `make` (and optionally `make dvid-basholeveldb`)
# first to populate bin/ with whatever you want installed; install will then
# copy any of INSTALLABLE_BINS that exist in bin/.
install:
	@install -d ${INSTALL_PREFIX}/bin; \
	installed=0; \
	for b in ${INSTALLABLE_BINS}; do \
	    if [ -f bin/$$b ]; then \
	        install bin/$$b ${INSTALL_PREFIX}/bin/$$b && \
	        echo "installed bin/$$b -> ${INSTALL_PREFIX}/bin/$$b" && \
	        installed=$$((installed+1)); \
	    fi; \
	done; \
	if [ $$installed -eq 0 ]; then \
	    echo ""; \
	    echo "ERROR: No installable binaries found in bin/."; \
	    echo "       Run 'make' (or 'make dvid', 'make tools', 'make dvid-basholeveldb')"; \
	    echo "       before 'make install'."; \
	    exit 1; \
	fi; \
	echo "Installed $$installed executable(s) to ${INSTALL_PREFIX}/bin."

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
bin/dvid: cmd/dvid/main.go server/version.go .last-build-git-description ${DVID_SOURCES} | bin ${DVID_BUILD_GUARDS}
	go build -o bin/dvid -v -tags "${DVID_TAGS}" cmd/dvid/main.go
	@echo "DVID executable at $(abspath bin/dvid)."

bin/dvid-basholeveldb: export SDKROOT=$(HEADERPATH)
bin/dvid-basholeveldb: cmd/dvid/main.go server/version.go .last-build-git-description ${DVID_SOURCES} | bin require-conda-for-basholeveldb
	go build -o bin/dvid-basholeveldb -v -tags "${DVID_TAGS}" cmd/dvid/main.go
	@echo "DVID with legacy basholeveldb support at $(abspath bin/dvid-basholeveldb)."

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

.PHONY: all tools install dvid-basholeveldb clean
clean:
	rm -f bin/*
	rm -f server/version.go
	rm -f .last-build-git-description
