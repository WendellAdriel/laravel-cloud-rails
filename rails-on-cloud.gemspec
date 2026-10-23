# frozen_string_literal: true

require_relative "lib/laravel_cloud/version"

Gem::Specification.new do |spec|
  spec.name = "rails-on-cloud"
  spec.version = LaravelCloud::VERSION
  spec.authors = ["Laravel"]
  spec.summary = "Bring Laravel Cloud features to Ruby on Rails."
  spec.description = "A feature-based integration for running Rails on Laravel Cloud, whose first " \
                     "capability is Managed Queues as an Active Job adapter."
  spec.homepage = "https://laravel.com/cloud"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"

  spec.files = Dir["lib/**/*", "exe/*", "LICENSE.md", "README.md"]
  spec.bindir = "exe"
  spec.executables = ["laravel-cloud"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activejob", ">= 7.1"
  spec.add_dependency "aws-sdk-sqs", "~> 1.80"
  spec.add_dependency "redis", ">= 4.8", "< 6"
end
