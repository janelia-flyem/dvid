CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
project (dvid)

include (ExternalProject)

set (RUN_ENVIRONMENT "Workstation" CACHE TYPE STRING)
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release)
endif ()

################################################################################
# Check if BUILDEM_DIR has already been assigned.  If not, create a default.
set (BUILDEM_DIR "None" CACHE TYPE STRING)

if (${BUILDEM_DIR} STREQUAL "None")
    message (FATAL_ERROR "ERROR: Buildem directory (for all downloads & builds) should be specified via -DBUILDEM_DIR=<path> on cmake command line.")
endif ()

message ("BUILDEM downloads and builds will be placed here: ${BUILDEM_DIR}")

###############################################################################

###############################################################################
# Download and install buildem, if it isn't already in BUILDEM_DIR.
set (BUILDEM_REPO_DIR ${BUILDEM_DIR}/src/buildem)
if (NOT EXISTS ${BUILDEM_REPO_DIR}/python.cmake)
    message ("Installing buildem repo...")
    ExternalProject_Add(buildem
        PREFIX ${BUILDEM_DIR}
        GIT_REPOSITORY https://github.com/janelia-flyem/buildem.git
        UPDATE_COMMAND ""
        PATCH_COMMAND ""
        CONFIGURE_COMMAND "" 
        BUILD_COMMAND ""
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND ""
    )
    message ("\n**********************************************************\n")
    message ("\nAfter running make, you must re-run the cmake command once")
    message ("buildem has been downloaded!\n")
    message ("\n***********************************************************\n")
else ()
    ###############################################################################

    # Use modules from the downloaded buildem
    set (CMAKE_MODULE_PATH ${BUILDEM_REPO_DIR})
    message("Using cmake modules from ${BUILDEM_REPO_DIR}")

    # Download and compile dependencies.
    # NOTE: To prevent build of Go, you can set the environment variable CI_ENV.  This is
    #  particularly useful when using a continuous integration system with its own reasonable
    #  Go toolset.
    if (DEFINED ENV{CI_ENV})
        message("Building under $ENV{CI_ENV} continuous integration system.  Using standard Go.")
        set (golang_NAME "")
    else ()
        message("Including Go language build...")
        include (golang)
    endif ()

    # Download web console and API help
    message ("Downloading web console and API help...")
    ExternalProject_Add(dvid-console
        PREFIX ${BUILDEM_DIR}
        GIT_REPOSITORY https://github.com/janelia-flyem/dvid-console.git
        UPDATE_COMMAND ""
        PATCH_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
    )
    set (CONSOLE_DIR ${BUILDEM_DIR}/src/dvid-console)

    # TODO: use hdf5 after writing Go interface for simple read & write.
    #include (hdf5)

    # The name of the backend should be the same as the tag used for conditional build
    # (see "// +build leveldb" in storage/leveldb.go) and the custom_target name for the
    # dependencies.

    include (leveldb)
    include (basholeveldb)
    include (hyperleveldb)

    set (DVID_BACKEND "basholeveldb" CACHE TYPE STRING)
    
    # flag for building graph backend (currently just using specified key-value store
    set (DVID_GRAPHBACKEND "graphkeyvalue" CACHE TYPE STRING)

    # flag for building with FUSE support for key-value
    set (DVID_FUSE FALSE CACHE TYPE BOOL)
    if (${DVID_FUSE})
        set (DVID_BUILD_TAGS "fuse")
    endif ()
    
    message ("Using DVID_BACKEND: ${DVID_BACKEND}")

	# Defaults to standard leveldb
    if ("${DVID_BACKEND}" STREQUAL "lmdb")
        set (DVID_BACKEND_DEPEND    "gomdb")
        message ("Installing Lightning MDB with modified Go driver for DVID storage engine")
    elseif ("${DVID_BACKEND}" STREQUAL "hyperleveldb")
        set (DVID_BACKEND_DEPEND    ${hyperleveldb_NAME})
        message ("Installing HyperLevelDB for DVID storage engine.")
    elseif ("${DVID_BACKEND}" STREQUAL "leveldb")
        set (DVID_BACKEND           "leveldb")
        set (DVID_BACKEND_DEPEND    ${leveldb_NAME})
        message ("Installing standard Google leveldb for DVID storage engine")
    elseif ("${DVID_BACKEND}" STREQUAL "basholeveldb")
        set (DVID_BACKEND           "basholeveldb")
        set (DVID_BACKEND_DEPEND    ${basholeveldb_NAME})
        message ("Installing Basho-tuned leveldb for DVID storage engine")
        if (CMAKE_SYSTEM_NAME MATCHES "Linux")
            if (EXISTS "/etc/issue")
                file(READ "/etc/issue" LINUX_ISSUE)
                # Ubuntu
                if (LINUX_ISSUE MATCHES "Ubuntu")
                    message ("Detected Ubuntu system.  Using -lrt linker flag.")
                    set (GO_LDFLAGS "-Wl,--no-as-needed;-lrt")
                endif ()
            endif()
        endif()
    elseif ("${DVID_BACKEND}" STREQUAL "bolt")
        set (DVID_BACKEND_DEPEND    "gobolt")
        message ("Installing pure Go LMDB-inspired Bolt key-value store.")
    elseif ("${DVID_BACKEND}" STREQUAL "couchbase")
        message (FATAL_ERROR "Couchbase is currently not supported as a DVID storage engine.")
    endif ()


    set (DVID_GO     github.com/janelia-flyem/dvid)

    set (CGO_FLAGS   CGO_CFLAGS="-I${BUILDEM_INCLUDE_DIR};${GO_CFLAGS}";CGO_LDFLAGS="-L${BUILDEM_LIB_DIR};${GO_LDFLAGS}")

    add_custom_target (gopackages-install
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} -u github.com/janelia-flyem/go
        DEPENDS     ${golang_NAME} ${nanomsg_NAME}
        COMMENT     "Adding go package dependencies for janelia-flyem projects...")

    add_custom_target (gopackages
        git submodule init
        COMMAND     git submodule update
        DEPENDS     gopackages-install
        WORKING_DIRECTORY $ENV{GOPATH}/src/github.com/janelia-flyem/go
        COMMENT     "Updating required Go packages...")
    
    add_custom_target (gojsonschema
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/janelia-flyem/gojsonschema
        DEPENDS     ${golang_NAME}
        COMMENT     "Adding gojsonschema library...")

    add_custom_target (goji
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/zenazn/goji
        DEPENDS     ${golang_NAME}
        COMMENT     "Adding goji web routing library...")

    add_custom_target (context
        ${BUILDEM_ENV_STRING} go get ${GO_GET} code.google.com/p/go.net/context
        DEPENDS     ${golang_NAME}
        COMMENT     "Adding go.net context")

    set (DVID_GO_PACKAGES gopackages gojsonschema goji context)

    add_custom_target (nrsc
        ${BUILDEM_ENV_STRING} ${GO_ENV} go build -o ${BUILDEM_BIN_DIR}/nrsc
        DEPENDS     gopackages
        WORKING_DIRECTORY $ENV{GOPATH}/src/github.com/janelia-flyem/go/nrsc/nrsc
        COMMENT     "Building nrsc resource compiler...")
    
    if (${DVID_FUSE})
        add_custom_target (gofuse
            ${BUILDEM_ENV_STRING} go get ${GO_GET} bazil.org/fuse
            DEPENDS     ${golang_NAME}
            COMMENT     "Adding FUSE Go library...")
        set (DVID_GO_PACKAGES ${DVID_GO_PACKAGES} gofuse)
    endif()

    add_custom_target (gobolt
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/boltdb/bolt
        DEPENDS     ${golang_NAME}
        COMMENT     "Adding BoltDB package...")

    add_custom_target (gomdb
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/DocSavage/gomdb
        DEPENDS     ${golang_NAME}
        COMMENT     "Adding CGo Lightning MDB...")

    # Build DVID with chosen backend
    add_custom_target (dvid-exe
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid 
        -v -tags '${DVID_BACKEND} ${DVID_GRAPHBACKEND} ${DVID_BUILD_TAGS}' cmd/dvid/main.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     ${golang_NAME} ${nanomsg_NAME} ${DVID_BACKEND_DEPEND} ${DVID_GO_PACKAGES} ${hdf5_NAME}
        COMMENT     "Compiling and installing dvid executable...")

    # Build DVID with embedded console 
    add_custom_target (dvid
        ${BUILDEM_ENV_STRING} nrsc ${BUILDEM_BIN_DIR}/dvid dvid-console
        WORKING_DIRECTORY   ${BUILDEM_DIR}/src
        DEPENDS     dvid-exe nrsc dvid-console
        COMMENT     "Adding embedded console into dvid executable..."
    )

   add_custom_target (test
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -tags '${DVID_BACKEND} ${DVID_GRAPHBACKEND}' 
            ${DVID_GO}/...)

   add_custom_target (coverage
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -cover -tags '${DVID_BACKEND} ${DVID_GRAPHBACKEND}' 
            ${DVID_GO}/...)

   # Add benchmarking
   add_custom_target (test-bench
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -bench -i -tags '${DVID_BACKEND} ${DVID_GRAPHBACKEND}' 
            ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore)

   add_custom_target (bench
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -bench -tags '${DVID_BACKEND} ${DVID_GRAPHBACKEND}' 
            ${DVID_GO}/...
        DEPENDS test-bench)


    ###############################################################################
endif()
