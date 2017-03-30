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

    ExternalProject_Add(dvid-console-dist
        PREFIX ${BUILDEM_DIR}
        URL https://github.com/janelia-flyem/dvid-console/releases/download/v1.13.1/dvid-console-1.13.1.tar.gz
        UPDATE_COMMAND ""
        PATCH_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
    )

    # TODO: use hdf5 after writing Go interface for simple read & write.
    #include (hdf5)

    # The name of the backend should be the same as the tag used for conditional build
    # (see "// +build leveldb" in storage/leveldb.go) and the custom_target name for the
    # dependencies.

    include (leveldb)
    include (basholeveldb)
    include (hyperleveldb)

    # This is the default local storage backend.
    set (DVID_BACKEND "basholeveldb" CACHE TYPE STRING)
        
    message ("Using DVID_BACKEND: ${DVID_BACKEND}")

    # Make sure we have list of all Go package dependencies that we are go getting.
    set (DVID_DEP_GO_PACKAGES gopackages gojsonschema goji context lumberjack snappy oauth2 gorpc groupcache)

    # Make sure we have all dependencies for the backend
	# Defaults to standard leveldb
    foreach (BACKEND ${DVID_BACKEND})
       if ("${BACKEND}" STREQUAL "lmdb")
            set (DVID_BACKEND_DEPEND  "gomdb" ${DVID_BACKEND_DEPEND})
            message ("Installing Lightning MDB with modified Go driver for DVID storage engine")
        elseif ("${BACKEND}" STREQUAL "kvautobus")
            set (DVID_DEP_GO_PACKAGES   ${DVID_DEP_GO_PACKAGES} msgp)
            message ("Installing Janelia KVAutobus for DVID storage engine.")
        elseif ("${BACKEND}" STREQUAL "hyperleveldb")
            set (DVID_BACKEND_DEPEND    ${hyperleveldb_NAME} ${DVID_BACKEND_DEPEND})
            message ("Installing HyperLevelDB for DVID storage engine.")
        elseif ("${BACKEND}" STREQUAL "leveldb")
            set (DVID_BACKEND_DEPEND    ${leveldb_NAME} ${DVID_BACKEND_DEPEND})
            message ("Installing standard Google leveldb for DVID storage engine")
        elseif ("${BACKEND}" STREQUAL "basholeveldb")
           set (DVID_BACKEND_DEPEND    ${basholeveldb_NAME} ${DVID_BACKEND_DEPEND})
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
        elseif ("${BACKEND}" STREQUAL "bolt")
            set (DVID_BACKEND_DEPEND    "gobolt" ${DVID_BACKEND_DEPEND})
            message ("Installing pure Go LMDB-inspired Bolt key-value store.")
        elseif ("${BACKEND}" STREQUAL "bigtable")
            set (DVID_DEP_GO_PACKAGES   ${DVID_DEP_GO_PACKAGES} gcloud)
            message ("Installing Google's Cloud BigTable key-value store.")
        elseif ("${BACKEND}" STREQUAL "gbucket")
            set (DVID_DEP_GO_PACKAGES   ${DVID_DEP_GO_PACKAGES} gcloud)
            message ("Installing Google's Bucket store.")
        elseif ("${BACKEND}" STREQUAL "couchbase" ${DVID_BACKEND_DEPEND})
            message (FATAL_ERROR "Couchbase is currently not supported as a DVID storage engine.")
        endif ()
    endforeach (BACKEND)


    set (DVID_GO     github.com/janelia-flyem/dvid)

    set (CGO_FLAGS   CGO_CFLAGS="-I${BUILDEM_INCLUDE_DIR};${GO_CFLAGS}";CGO_LDFLAGS="-L${BUILDEM_LIB_DIR};${GO_LDFLAGS}")

    add_custom_target (gopackages-install
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} -u github.com/janelia-flyem/go
        COMMENT     "Adding go package dependencies for janelia-flyem projects...")

    add_custom_target (gopackages
        git submodule init
        COMMAND     git submodule update
        DEPENDS     gopackages-install
        WORKING_DIRECTORY $ENV{GOPATH}/src/github.com/janelia-flyem/go
        COMMENT     "Updating required Go packages...")

    # add_custom_target (goprotobuf
    #     ${BUILDEM_ENV_STRING} go get ${GO_GET} -u github.com/golang/protobuf/{proto,protoc-gen-go}
    #     COMMENT     "Adding goprotobuf support...")
    
    add_custom_target (gojsonschema
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/janelia-flyem/gojsonschema
        COMMENT     "Adding gojsonschema library...")

    add_custom_target (goji
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/zenazn/goji
        COMMENT     "Adding goji web routing library...")

    add_custom_target (msgp
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/tinylib/msgp
        COMMENT     "Adding msgp messagepack library...")

    add_custom_target (context
        ${BUILDEM_ENV_STRING} go get ${GO_GET} golang.org/x/net/context
        COMMENT     "Adding go.net context")

    add_custom_target (lumberjack
        ${BUILDEM_ENV_STRING} go get ${GO_GET} gopkg.in/natefinch/lumberjack.v2
        COMMENT     "Adding lumberjack library...")

    add_custom_target (snappy
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/golang/snappy
        COMMENT     "Adding snappy library...")

    add_custom_target (groupcache
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/golang/groupcache
        COMMENT     "Adding groupcache library...")

    add_custom_target (oauth2
        ${BUILDEM_ENV_STRING} go get ${GO_GET} golang.org/x/oauth2
        COMMAND ${BUILDEM_ENV_STRING} go get ${GO_GET} cloud.google.com/go/compute/metadata
        COMMENT     "Adding oauth2 library...")

    add_custom_target (gcloud
        COMMAND ${BUILDEM_ENV_STRING} go get ${GO_GET} cloud.google.com/go/bigtable
        COMMAND ${BUILDEM_ENV_STRING} go get ${GO_GET} cloud.google.com/go/storage
        COMMAND ${BUILDEM_ENV_STRING} go get ${GO_GET} google.golang.org/api/option
        COMMAND ${BUILDEM_ENV_STRING} go get ${GO_GET} google.golang.org/grpc
        COMMAND ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/golang/protobuf/{proto,protoc-gen-go}
        COMMENT     "Adding gcloud packages...")  

    add_custom_target (gorpc
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/valyala/gorpc
        COMMENT     "Adding gorpc library...")

    add_custom_target (nrsc
        ${BUILDEM_ENV_STRING} ${GO_ENV} go build -o ${BUILDEM_BIN_DIR}/nrsc
        DEPENDS     gopackages
        WORKING_DIRECTORY $ENV{GOPATH}/src/github.com/janelia-flyem/go/nrsc/nrsc
        COMMENT     "Building nrsc resource compiler...")
    
    if (${DVID_FUSE})
        add_custom_target (gofuse
            ${BUILDEM_ENV_STRING} go get ${GO_GET} bazil.org/fuse
            COMMENT     "Adding FUSE Go library...")
        set (DVID_DEP_GO_PACKAGES ${DVID_DEP_GO_PACKAGES} gofuse)
    endif()

    add_custom_target (gobolt
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/boltdb/bolt
        COMMENT     "Adding BoltDB package...")

    add_custom_target (gomdb
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/DocSavage/gomdb
        COMMENT     "Adding CGo Lightning MDB...")

    # Compile command to generate DVID source code version info.
    add_custom_target (dvid-gen-version
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid-gen-version 
        -v -tags '${DVID_BACKEND}' cmd/gen-version/main.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT     "Compiling and installing dvid version generation command...")

    # Handle DVID code generation before we build the main DVID executable.
    add_custom_target (dvid-code-gen
        ${BUILDEM_BIN_DIR}/dvid-gen-version -o ${CMAKE_CURRENT_SOURCE_DIR}/server/version.go
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     dvid-gen-version
        COMMENT     "Generating extra DVID code before building main executable...")

    # Build DVID with chosen backend
    add_custom_target (dvid
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid 
        -v -tags '${DVID_BACKEND}' cmd/dvid/main.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     ${DVID_BACKEND_DEPEND} ${DVID_DEP_GO_PACKAGES} dvid-code-gen dvid-backup dvid-transfer
        COMMENT     "Compiling and installing dvid executable...")

    # # Build DVID with embedded console 
    # add_custom_target (dvid
    #     ${BUILDEM_ENV_STRING} nrsc ${BUILDEM_BIN_DIR}/dvid dvid-console
    #     WORKING_DIRECTORY   ${BUILDEM_DIR}/src
    #     DEPENDS     dvid-exe nrsc dvid-console dvid-backup dvid-transfer
    #     COMMENT     "Adding embedded console into dvid executable..."
    # )

    set (DVID_PACKAGES ${DVID_GO}/dvid ${DVID_GO}/storage/... ${DVID_GO}/datastore ${DVID_GO}/server
                       ${DVID_GO}/datatype/... ${DVID_GO}/tests_integration)

   add_custom_target (test
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -tags '${DVID_BACKEND}' 
            ${DVID_PACKAGES})

   add_custom_target (test-verbose
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -v -tags '${DVID_BACKEND}' 
            ${DVID_PACKAGES})

   add_custom_target (test-labelvol
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -v -tags '${DVID_BACKEND}' 
            ${DVID_GO}/datatype/labelvol)

   add_custom_target (coverage
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -cover -tags '${DVID_BACKEND}' 
            ${DVID_PACKAGES})

   # Add benchmarking
   add_custom_target (test-bench
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -bench -i -tags '${DVID_BACKEND}' 
            ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore)

   add_custom_target (bench
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -bench -tags '${DVID_BACKEND}' 
            ${DVID_PACKAGES}
        DEPENDS test-bench)

    # Build backup command 
    add_custom_target (dvid-backup
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid-backup 
        -v -tags '${DVID_BACKEND}' cmd/backup/main.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT     "Compiling and installing dvid backup command...")

    # Build transfer command 
    add_custom_target (dvid-transfer
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid-transfer 
        -v -tags '${DVID_BACKEND}' cmd/transfer/*.go
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT     "Compiling and installing dvid transfer command...")

    # Create dvid-distro directory off buildem directory
    set (DISTRO_DIR ${BUILDEM_DIR}/dvid-distro)
    add_custom_target (mkdir-distro
        COMMAND ${CMAKE_COMMAND} -E make_directory ${DISTRO_DIR}
        DEPENDS dvid-console-dist
        COMMENT     "Creating dvid distro directory in ${BUILDEM_DIR} ...")  
    
    # Unpack dvid console web app 
    add_custom_target (console-distro
        COMMAND ${CMAKE_COMMAND} -E make_directory ${DISTRO_DIR}
        COMMAND ${CMAKE_COMMAND} -E tar zxf ${BUILDEM_DIR}/src/dvid-console-1.13.1.tar.gz
        WORKING_DIRECTORY ${DISTRO_DIR}
        DEPENDS mkdir-distro
        COMMENT     "Creating dvid console in distro...")  

    # Build binary distribution
    if (${APPLE})
        set (LAUNCH_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/dist/launch_dvid_mac")
    else()
        set (LAUNCH_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/dist/launch_dvid_linux")
    endif()

    add_custom_target (distro
        COMMAND ${CMAKE_COMMAND} -E copy ${LAUNCH_SCRIPT} ${DISTRO_DIR}/launch_dvid
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dist/README ${DISTRO_DIR}/README
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/config-simple.toml ${DISTRO_DIR}/config-simple.toml
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/config-full.toml ${DISTRO_DIR}/config-full.toml
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${BUILDEM_BIN_DIR} ${DISTRO_DIR}/bin 
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${BUILDEM_LIB_DIR} ${DISTRO_DIR}/lib 
        COMMAND ${CMAKE_COMMAND} -E tar cfz dvid-release.tar.gz dvid-distro
        WORKING_DIRECTORY ${BUILDEM_DIR}
        DEPENDS     console-distro
        COMMENT     "Creating binary distro...")  


    ###############################################################################
endif()
