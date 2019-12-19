cmake_minimum_required(VERSION 3.15)
project(final_project C)

set(CMAKE_C_STANDARD 99)

find_package(Threads)

add_executable(master master.c lib/slavelist.h lib/utilities.h)
add_executable(slave slave.c lib/utilities.h)
add_executable(client client.c lib/utilities.h)
add_executable(countwords jobs/count-words/countwords.c)

target_link_libraries(master ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(slave ${CMAKE_THREAD_LIBS_INIT})
