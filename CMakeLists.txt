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
        GIT_REPOSITORY http://github.com/janelia-flyem/buildem.git
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
        GIT_REPOSITORY http://github.com/janelia-flyem/dvid-webclient.git
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

    include (protobuf)
    include (leveldb)

    # The name of the backend should be the same as the tag used for conditional build
    # (see "// +build levigo" in storage/levigo.go) and the custom_target name for the
    # dependencies.
    set (DVID_BACKEND levigo)
    set (DVID_BACKEND_DEPEND ${leveldb_NAME})

    set (DVID_GO      github.com/janelia-flyem/dvid)

    set (CGO_FLAGS   CGO_CFLAGS="-I${BUILDEM_INCLUDE_DIR}";CGO_LDFLAGS="-L${BUILDEM_LIB_DIR};-lsnappy")

    add_custom_target (gopackages-install
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} github.com/janelia-flyem/go
        DEPENDS     ${golang_NAME} ${protobuf_NAME}
        COMMENT     "Added go package dependencies for janelia-flyem projects")

    add_custom_target (gopackages
        git submodule init
        COMMAND     git submodule update
        DEPENDS     gopackages-install
        WORKING_DIRECTORY $ENV{GOPATH}/src/github.com/janelia-flyem/go
        COMMENT     "Updated required Go packages")
    
    add_custom_target (emdata
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/DocSavage/emdata
        DEPENDS     ${golang_NAME}
        COMMENT     "Added emdata package from DocSavage repo")
    
    add_custom_target (extensions
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} github.com/DocSavage/dvid
        DEPENDS     ${golang_NAME} emdata
        COMMENT     "Added DVID extensions from DocSavage repo")
    
    #add_custom_target (snappy-go
    #    ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} code.google.com/p/snappy-go/snappy
    #    DEPENDS     ${golang_NAME} ${snappy_NAME}
    #    COMMENT     "Added Go bindings to snappy [code.google.com/p/snappy-go/snappy]")
    
    #add_custom_target (nrsc
    #    ${BUILDEM_ENV_STRING} go get ${GO_GET} bitbucket.org/tebeka/nrsc
    #    DEPENDS     ${golang_NAME}
    #    COMMENT     "Added nrsc tool for adding zip file to go executable")
    
    #add_custom_target (uuid
    #    ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} code.google.com/p/go-uuid/uuid
    #    DEPENDS     ${golang_NAME}
    #    COMMENT     "Added Go uuid package [code.google.com/p/go-uuid/uuid]")
    
    #add_custom_target (config
    #    ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} github.com/robfig/config
    #    DEPENDS     ${golang_NAME}
    #    COMMENT     "Added ini config file handler [github.com/robfig/config]")
    
    #add_custom_target (levigo
    #    ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go get ${GO_GET} github.com/jmhodges/levigo
    #    DEPENDS     ${golang_NAME} ${leveldb_NAME}
    #    COMMENT     "Added Go bindings to leveldb [github.com/jmhodges/levigo]")
    
    #add_custom_target (goprotobuf
    #    ${BUILDEM_ENV_STRING} go get ${GO_GET} code.google.com/p/goprotobuf/proto
    #    DEPENDS     ${golang_NAME} ${protobuf_NAME}
    #    COMMENT    "Add Go protobuf support")
    
    add_custom_target (goleveldb
        ${BUILDEM_ENV_STRING} go get ${GO_GET} github.com/syndtr/goleveldb/leveldb
        DEPENDS     ${golang_NAME} snappy-go
        COMMENT     "Added pure Go implementation of leveldb")

    # Build DVID with chosen backend
    add_custom_target (dvid-exe
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${CGO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid 
            -v -tags '${DVID_BACKEND}' dvid.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     ${golang_NAME} ${snappy_NAME} ${DVID_BACKEND_DEPEND}
                    gopackages extensions
        COMMENT     "Compiled and installed dvid executable.")

    # Build DVID with pure Go leveldb implementation
    #add_custom_target (dvid-go-exe
    #    ${BUILDEM_ENV_STRING} ${GO_ENV} go build -o ${BUILDEM_BIN_DIR}/dvid-go 
    #        -v -tags 'goleveldb' dvid.go 
    #    WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
    #    DEPENDS     ${golang_NAME} snappy-go nrsc uuid goleveldb goprotobuf
    #    COMMENT     "Compiled and installed dvid pure Go executable, dvid-go.")

   # Embed web client into exe
   set (NRSC_SCRIPT "$ENV{GOPATH}/src/github.com/janelia-flyem/go/nrsc/nrsc")
   add_custom_target (dvid
        ${NRSC_SCRIPT} ${BUILDEM_BIN_DIR}/dvid ${WEBCLIENT_DIR}
        DEPENDS dvid-exe dvid-webclient
        COMMENT     "Added webclient in ${WEBCLIENT_DIR} to dvid executable")
   
   #add_custom_target (dvid-go
   #     ${NRSC_SCRIPT} ${BUILDEM_BIN_DIR}/dvid-go ${CMAKE_CURRENT_SOURCE_DIR}/webclient
   #     DEPENDS dvid-go-exe
   #     COMMENT     "Added files in ${CMAKE_CURRENT_SOURCE_DIR}/webclient to dvid-go executable")

   # Add testing
   add_custom_target (test-build
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -i -tags '${DVID_BACKEND}' 
            ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore)

   add_custom_target (test
        ${BUILDEM_ENV_STRING} ${CGO_FLAGS} go test -tags '${DVID_BACKEND}' 
            ${DVID_GO}/test ${DVID_GO}/dvid ${DVID_GO}/datastore
        DEPENDS test-build)


    ###############################################################################
endif()
