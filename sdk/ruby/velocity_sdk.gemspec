# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name          = 'velocity_sdk'
  spec.version       = '0.1.0'
  spec.authors       = ['UnitBuilds']
  spec.email         = ['ian@unitbuilds.com']

  spec.summary       = 'Ruby SDK for the VELOCITY-WorkFlow engine'
  spec.description   = 'Ruby client for the VELOCITY-WorkFlow engine — uses FFI to call the native ' \
                        'velocity_workflow_engine shared library, or gRPC for remote connections.'
  spec.homepage      = 'https://github.com/velocity/workflow-sdk'
  spec.license       = 'MIT'
  spec.required_ruby_version = '>= 3.0.0'

  spec.files         = Dir['lib/**/*.rb', 'LICENSE', 'README.md']
  spec.require_paths = ['lib']

  spec.add_dependency 'ffi', '~> 1.16'

  spec.add_development_dependency 'rspec', '~> 3.12'
  spec.add_development_dependency 'rake', '~> 13.0'
end
