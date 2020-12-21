############## ftl ###############
set(PROJNAME dataframe)
project(${PROJNAME})

file(GLOB SRC
    *.cpp
    *.h
)

add_library( ${PROJNAME} STATIC ${SRC} )
target_compile_features( ${PROJNAME} PUBLIC cxx_std_17)
target_include_directories( ${PROJNAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} )


