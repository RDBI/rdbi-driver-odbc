Gem::Specification.new do |s|
  s.name        = "rdbi-driver-odbc"
  s.version     = "0.1.2"
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Shane Emmons"]
  s.email       = "semmons99@gmail.com"
  s.homepage    = "https://github.com/semmons99/rdbi-driver-odbc"
  s.summary     = "ODBC driver for RDBI"
  s.description = "This gem gives you the ability to query ODBC connections with RDBI."

  s.required_rubygems_version = ">= 1.3.6"

  s.add_dependency "rdbi",      "~> 0.9"
  s.add_dependency "ruby-odbc", "= 0.99993"

  s.add_development_dependency "rdbi-dbrc", "~> 0.1"
  s.add_development_dependency "rspec",     "~> 2"
  s.add_development_dependency "yard"

  s.files        = Dir.glob("lib/**/*") + %w(CHANGELOG.md LICENSE README.md)
  s.require_path = "lib"
end
