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

    # Use modules from the downloaded buildem
    set (CMAKE_MODULE_PATH ${BUILDEM_REPO_DIR})
    message("Using cmake modules from ${BUILDEM_REPO_DIR}")

    # Download and compile dependencies
    include (leveldb)
    include (golang)

    set (GO_FLAGS   CGO_CFLAGS="-I${BUILDEM_INCLUDE_DIR}";CGO_LDFLAGS="-L${BUILDEM_LIB_DIR};-lsnappy")
    add_custom_target (snappy-go
        ${BUILDEM_ENV_STRING} ${GO_FLAGS} go get code.google.com/p/snappy-go/snappy
        DEPENDS     ${golang_NAME} ${snappy_NAME}
        COMMENT     "Added Go bindings to snappy [code.google.com/p/snappy-go/snappy]")
    
    add_custom_target (nrsc
        ${BUILDEM_ENV_STRING} go get bitbucket.org/tebeka/nrsc
        DEPENDS     ${golang_NAME}
        COMMENT     "Added nrsc tool for adding zip file to go executable")
    
    add_custom_target (levigo
        ${BUILDEM_ENV_STRING} ${GO_FLAGS} go get github.com/jmhodges/levigo
        DEPENDS     ${golang_NAME} ${leveldb_NAME}
        COMMENT     "Added Go bindings to leveldb [github.com/jmhodges/levigo]")
    
    add_custom_target (goleveldb
        ${BUILDEM_ENV_STRING} go get github.com/syndtr/goleveldb/leveldb
        DEPENDS     ${golang_NAME} snappy-go
        COMMENT     "Added pure Go implementation of leveldb")
    
    # Build DVID with C++ leveldb library support
    add_custom_target (dvid-exe
        ${BUILDEM_ENV_STRING} ${GO_ENV} ${GO_FLAGS} go build -o ${BUILDEM_BIN_DIR}/dvid -v dvid.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     ${golang_NAME} ${leveldb_NAME} nrsc levigo
        COMMENT     "Compiled and installed dvid executable.")

    # Build DVID with pure Go leveldb implementation
    add_custom_target (dvid-go-exe
        ${BUILDEM_ENV_STRING} ${GO_ENV} go build -o ${BUILDEM_BIN_DIR}/dvid-go -v -tags 'purego' dvid.go 
        WORKING_DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS     ${golang_NAME} nrsc goleveldb
        COMMENT     "Compiled and installed dvid pure Go executable, dvid-go.")

   # Embed web client into exe
   set (NRSC_SCRIPT "$ENV{GOPATH}/src/bitbucket.org/tebeka/nrsc/nrsc")
   add_custom_target (dvid
        ${NRSC_SCRIPT} ${BUILDEM_BIN_DIR}/dvid ${CMAKE_CURRENT_SOURCE_DIR}/webclient
        DEPENDS dvid-exe
        COMMENT     "Added files in ${CMAKE_CURRENT_SOURCE_DIR}/webclient to dvid executable")
   
   add_custom_target (dvid-go
        ${NRSC_SCRIPT} ${BUILDEM_BIN_DIR}/dvid-go ${CMAKE_CURRENT_SOURCE_DIR}/webclient
        DEPENDS dvid-go-exe
        COMMENT     "Added files in ${CMAKE_CURRENT_SOURCE_DIR}/webclient to dvid-go executable")


    ###############################################################################
endif()
