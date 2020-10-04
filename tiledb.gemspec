require_relative 'lib/tiledb/version'

Gem::Specification.new do |spec|
  spec.name          = "tiledb"
  spec.version       = TileDB::VERSION
  spec.authors       = ["holrock"]
  spec.email         = ["ispeporez@gmail.com"]

  spec.summary       = %q{TileDB ruby binding}
  spec.description   = %q{TileDB ruby binding}
  spec.homepage      = "https://github.com/holrock/tiledb-ruby"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = spec.homepage

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake-compiler"
  spec.add_development_dependency "test-unit"
  spec.extensions = %w[ext/tiledb/extconf.rb]
end
