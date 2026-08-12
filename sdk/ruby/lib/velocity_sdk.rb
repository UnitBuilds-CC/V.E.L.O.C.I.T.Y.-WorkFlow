# frozen_string_literal: true

# VELOCITY-WorkFlow Ruby SDK
#
# Ruby client for the VELOCITY-WorkFlow engine. Uses the `ffi` gem to call
# the native velocity_workflow_engine shared library (.dll / .so / .dylib),
# or falls back to gRPC for remote connections.
#
# Usage:
#   require 'velocity_sdk'
#
#   client = VelocitySdk::VelocityClient.new("localhost:50051")
#   key = client.start_workflow("my-workflow", total_steps: 5)
#   client.signal_workflow(key, "my-signal", "payload")
#   status = client.get_status(key)
#   client.close

require_relative 'velocity_sdk/errors'
require_relative 'velocity_sdk/interceptors'
require_relative 'velocity_sdk/client'
require_relative 'velocity_sdk/testing'
require_relative 'velocity_sdk/retry'
require_relative 'velocity_sdk/payload_codec'
require_relative 'velocity_sdk/workflow_stub'
require_relative 'velocity_sdk/workflow_execution'
require_relative 'velocity_sdk/workflow_options'
require_relative 'velocity_sdk/activity_options'
require_relative 'velocity_sdk/activity_stub'
require_relative 'velocity_sdk/connection'

module VelocitySdk
  # Gem version, kept in sync with the gemspec.
  VERSION = '0.1.0'
end
