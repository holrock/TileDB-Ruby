require "bundler/gem_tasks"
require "rake/extensiontask"
require "rake/testtask"

Rake::ExtensionTask.new("tiledb") do |ext|
  ext.lib_dir = "lib/tiledb"
end

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList['test/test_*.rb']
  t.verbose = true
end

task :test => :compile
task :default => :test
