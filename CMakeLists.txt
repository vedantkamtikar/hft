cmake_minimum_required(VERSION 3.20)

project(
    low_latency_pipeline
    VERSION 1.0.0
    DESCRIPTION "Sub-microsecond 3-Phase Market Data and Execution Pipeline in C++20"
    LANGUAGES CXX
)

# Standard C++20 requirement
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimization and warning flags
if(MSVC)
    add_compile_options(
        /O2                 # Maximum optimization (favor speed)
        /Oi                 # Enable intrinsic functions
        /Ot                 # Favor fast code
        /Oy                 # Enable frame pointer omission
        /Ob2                # Aggressive inline expansion
        /permissive-        # Strict standard conformance
        /W4                 # Warning level 4
        /wd4201             # Nameless struct/union extension
        /wd4324             # Structure padded due to alignment specifier (intentional alignas 64)
    )
    add_compile_definitions(
        WIN32_LEAN_AND_MEAN
        NOMINMAX
        _CRT_SECURE_NO_WARNINGS
    )
else()
    add_compile_options(
        -O3
        -march=native
        -Wall
        -Wextra
        -Wpedantic
        -Wno-unused-parameter
    )
endif()

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# System and library dependencies
find_package(Threads REQUIRED)

# Low Latency Pipeline executable
add_executable(low_latency_pipeline src/main.cpp)

target_include_directories(low_latency_pipeline PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(low_latency_pipeline PRIVATE
    Threads::Threads
)

# Windows WinSock2 network and multimedia timer library
if(WIN32)
    target_link_libraries(low_latency_pipeline PRIVATE ws2_32 winmm)
endif()
