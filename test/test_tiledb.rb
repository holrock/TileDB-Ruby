require 'test/unit'
require_relative '../lib/tiledb'

class TileDBTest < Test::Unit::TestCase
  def test_runtime_version
    v = TileDB.runtime_version
    assert_equal 3, v.size
    assert 1 < v[0]
    # assert_equal 0, v[1]
    # assert_equal 8, v[2]
  end

  def test_dim_name
      dim = TileDB::Dim.new("rows", [1, 4], 4)
      assert_equal "rows", dim.name
  end

  def test_quick_dense
    array_name = "tmp/quickstart_dense"
    dim1 = TileDB::Dim.new("rows", [1, 4], 4)
    dim2 = TileDB::Dim.new("cols", [1, 4], 4)
    dom = TileDB::Domain.new([dim1, dim2])
    assert_not_equal nil, dom

    schema = TileDB::ArraySchema.new(dom, [TileDB::Attr.new("a", :int32)])
    assert_not_equal nil, schema
    assert_nothing_raised do
      TileDB::DenseArray.create(array_name, schema)
    end
    assert_nothing_raised do
      TileDB::DenseArray.new(array_name, mode = "w") do |a|
        data = [[1,2,3,4],
                [5,6,7,8],
                [9,10,11,12],
                [13,14,15,16]]
        a.store(data)
      end
    end
  end
end
