# Determine ERL include path at runtime
ERL_INCLUDE_PATH := $(shell erl -noshell -eval 'io:format("~s", [code:root_dir() ++ "/usr/include"])' -s init stop)

# Check if ERL_INCLUDE_PATH is valid
ifeq ($(strip $(ERL_INCLUDE_PATH)),)
$(error Failed to determine ERL include path. Ensure Erlang is installed and accessible.)
endif

# Source and target files
NIF_SRC := c_src/clock_wrapper.c
NIF_SO := priv/clock_wrapper.so

# Compiler flags
CFLAGS := -fPIC -shared -dynamiclib -undefined dynamic_lookup -I $(ERL_INCLUDE_PATH)

# Compile the NIF
compile-nif:
	@mkdir -p priv
	clang $(CFLAGS) -o $(NIF_SO) $(NIF_SRC)
	@echo "NIF compiled successfully."

clean-nif:
	rm -rf priv/*
