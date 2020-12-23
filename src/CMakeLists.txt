############## ftl ###############
set(PROJNAME zjdataframe)
project(${PROJNAME})

file(GLOB SRC
    zj/*.cpp
    zj/*.h
)

add_library( ${PROJNAME} STATIC ${SRC} )
target_compile_features( ${PROJNAME} PUBLIC cxx_std_17)
target_include_directories( ${PROJNAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} )


