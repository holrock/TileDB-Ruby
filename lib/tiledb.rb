require "tiledb/version"

require 'tiledb/tiledb'

module TileDB
  class Error < StandardError; end
  class OOMError < StandardError; end
  class Config
  end

  class Ctx
    def self.default(config = nil)
      @default_ctx ||= Ctx.new(config)
    end

    def initialize(config = nil)
      cinit(config)
      @config = config
    end

    def inspect
      "#<TileDB::Ctx @config=#{@config}>"
    end
  end

  class Domain
    def initialize(dims, ctx = Ctx.default)
      cinit(dims, ctx)
      @dims = dims
    end

    def inspect
      "#<TileDB::Domain @dims=#{@dims}>"
    end
  end

  class Dim
    def initialize(name, domain, tile, dtype = :int32, ctx = Ctx.default)
      cinit(name, domain, tile, dtype, ctx)
    end

    def inspect
      "#<TileDB::Dim @name=#{name}>"
    end
  end

  class Attr
    def initialize(name, dtype, var = false, filters = nil, ctx = Ctx.default)
      cinit(name, dtype, var, filters, ctx)
    end
  end

  class ArraySchema
    attr_reader :ctx

    def initialize(domain, attrs, cell_order = ROW_MAJOR, tile_order = ROW_MAJOR, capacity = 0, coords_filter = nil,
                   offsets_filter = nil, allow_duplicates = false, sparse = false, ctx = Ctx.default)
      cinit(domain, attrs, cell_order, tile_order, capacity, coords_filter, offsets_filter, allow_duplicates, sparse, ctx);
      @ctx = ctx
    end

    def inspect
      "#<TileDB::ArraySchema @ctx=#{@ctx}>"
    end
  end

  class Array
    def self.create(uri, schema, key = nil, ctx = nil)
      ctx = ctx || schema.ctx || Ctx.default
      create_impl(uri, schema, key, ctx)
    end

    def initialize(uri, mode = "r", key = nil, timestamp = nil, attr = nil, ctx = nil)
      ctx = ctx || Ctx.default
      cinit(uri, mode, key, timestamp, attr, ctx)
      @ctx = ctx
      @uri = uri;
      @mode = mode
      @key = key
      if block_given?
        begin
          yield(self)
        ensure
          close
        end
      end
    end

    def store(data)

    end
  end

  class DenseArray < Array
  end
end

