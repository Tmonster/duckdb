if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/Tmonster/duckdb-aws
            GIT_TAG 6498381d5a45aa00d266f2284ecf18dbbce64240
            )
endif()
