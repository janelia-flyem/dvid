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

    # Download the DVID webclient from its separate repository
    message ("Installing buildem repo...")
    ExternalProject_Add(dvid-webclient
        PREFIX ${BUILDEM_DIR}
        GIT_REPOSITORY https://github.com/janelia-flyem/dvid-webclient.git
        UPDATE_COMMAND ""
        PATCH_COMMAND ""
        CONFIGURE_COMMAND "" 
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
    )
    set (WEBCLIENT_DIR ${BUILDEM_DIR}/src/dvid-webclient)

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

    # TODO: use hdf5 after writing Go interface for simple read & write.
    #include (hdf5)

    # The name of the backend should be the same as the tag used for conditional build
    # (see "// +build leveldb" in storage/leveldb.go) and the custom_target name for the
    # dependencies.

    include (leveldb)
    include (basholeveldb)
    include (hyperleveldb)
    include (lmdb)

    if (NOT DVID_BACKEND)
        set (DVID_BACKEND "basholeveldb")
    endif ()

	# Defaults to standard leveldb
    if ("${DVID_BACKEND}" STREQUAL "lmdb")
        set (DVID_BACKEND_DEPEND    ${lmdb_NAME})
        message ("Installing Lightning MDB for DVID storage engine")
    elseif ("${DVID_BACKEND}" STREQUAL "couchbase")
        set (DVID_BACKEND_DEPEND    "")   # Assume manual install
        message ("Assuming manually installed couchbase will be used for DVID storage engine.")
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
                    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lrt")
                endif ()
            endif()
        endif()
    endif ()


    set (DVID_GO     github.com/janelia-flyem/dvid)

    set (CGO_FLAGS   CGO_CFLAGS="-I${BUILDEM_INCLUDE_DIR}";CGO_LDFLAGS="-L${BUILDEM_LIB_DIR}")

    add_custom_target (gopackages-install
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} github.com/janelia-flyem/go
        DEPENDS     ${golang_NAME}
        COMMENT     "Added go package dependencies for janelia-flyem projects")

    add_custom_target (gopackages
        git submodule init
        COMMAND     git submodule update
        DEPENDS     gopackages-install
        WORKING_DIRECTORY $ENV{GOPATH}/src/github.com/janelia-flyem/go
        COMMENT     "Updated required Go packages")
    
    add_custom_target (gofuse
        ${BUILDEM_ENV_STRING} go get ${GO_GET} bazil.org/fuse
        DEPENDS     ${golang_NAME}
        COMMENT     "Added FUSE Go library")

    # Build DVID with chosen backend
    add_custom_target (dvid
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid 
            -v -tags '${DVID_BACKEND}' dvid.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     ${golang_NAME} ${DVID_BACKEND_DEPEND}
                    gopackages gofuse ${hdf5_NAME}
        COMMENT     "Compiled and installed dvid executable.")

   # Add testing
   add_custom_target (test-build
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -i -tags '${DVID_BACKEND}' 
            ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore)

   add_custom_target (test
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -tags '${DVID_BACKEND}' 
            ${DVID_GO}/...
        DEPENDS test-build)

   # Add benchmarking
   add_custom_target (test-bench
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -bench -i -tags '${DVID_BACKEND}' 
            ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore)

   add_custom_target (bench
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -bench -tags '${DVID_BACKEND}' 
            ${DVID_GO}/...
        DEPENDS test-bench)


    ###############################################################################
endif()
