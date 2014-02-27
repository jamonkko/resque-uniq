# -*- encoding: utf-8 -*-

lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'resque-uniq/version'

Gem::Specification.new do |gem|
  gem.authors       = ["Trung Duc Tran"]
  gem.email         = ["trung@tdtran.org"]
  gem.summary       = "A Resque plugin to ensure only one job instance is queued or running at a time."
  gem.homepage      = "http://github.com/tdtran/resque-uniq"

  Dir.chdir(File.dirname(__FILE__)) do
    gem.files       = Dir.glob("**/*", File::FNM_DOTMATCH).reject {|f| File.directory?(f)}
  end

  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "resque-uniq"
  gem.require_paths = ["lib"]
  gem.version       = ResqueUniq::VERSION
end
