cmake_minimum_required(VERSION 3.14)
project(nvdb_verify CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# Fetch NanoVDB at the exact fVDB-pinned commit
FetchContent_Declare(
    openvdb
    GIT_REPOSITORY https://github.com/AcademySoftwareFoundation/openvdb.git
    GIT_TAG        5f0432b3387c169212a009ddaa05fdd703016549
    GIT_SHALLOW    FALSE
)
# Only populate (download), do NOT configure/build the full OpenVDB project.
# NanoVDB is header-only — we just need the source tree.
FetchContent_Populate(openvdb)

add_executable(nvdb_verify nvdb_verify.cpp)

# NanoVDB headers live under nanovdb/ in the OpenVDB repo
target_include_directories(nvdb_verify PRIVATE
    ${openvdb_SOURCE_DIR}/nanovdb
)
