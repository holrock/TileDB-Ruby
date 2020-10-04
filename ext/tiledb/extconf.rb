require "mkmf"

dir_config("tiledb")
abort "miissing tiledb.h" unless have_header("tiledb/tiledb.h")
abort "miissing tiledb library" unless have_library("tiledb")

create_makefile("tiledb/tiledb")
