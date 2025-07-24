ERL_INCLUDE_PATH := $(shell erl -noshell -eval 'io:format("~s", [code:root_dir() ++ "/usr/include"])' -s init stop)

ifeq ($(strip $(ERL_INCLUDE_PATH)),)
  $(error Failed to determine ERL include path. Ensure Erlang is installed and accessible.)
endif

NIF_SRC := c_src/clock_monotonic.c
NIF_SO := $(BUILD_DIR)/priv/clock_monotonic.so
CFLAGS := -fPIC -shared -dynamiclib -undefined dynamic_lookup -I $(ERL_INCLUDE_PATH)

${NIF_SO}:
	@mkdir -p `dirname $(NIF_SO)`
	clang $(CFLAGS) -o $(NIF_SO) $(NIF_SRC)

clean:
	rm -rf $(NIF_SO)
