# frozen_string_literal: true

# Example: Basic workflow with signal and query using the VELOCITY-WorkFlow Ruby SDK.
#
# Demonstrates:
#   - Starting a workflow
#   - Sending signals
#   - Querying workflow state
#   - Completing the workflow
#
# Prerequisites:
#   1. Start the VELOCITY-WorkFlow server:
#      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
#   2. Install dependencies:
#      cd VELOCITY-WorkFlow/sdk/ruby && bundle install
#   3. Run this example:
#      ruby examples/basic_workflow.rb

require_relative '../lib/velocity_sdk'

puts '=== VELOCITY-WorkFlow Ruby SDK — Basic Workflow ==='
puts

client = VelocitySdk::VelocityClient.new(target: 'localhost:50051')

begin
  # 1. Start a workflow
  key = client.start_workflow(
    'order-processing',
    namespace: 'default',
    task_queue: 'orders',
    total_steps: 3,
    input: '{"order_id": 12345}',
  )
  puts "1. Workflow started: key=#{key}"

  # 2. Get the workflow status
  status = client.get_status(key)
  puts "2. Status: #{status}"

  # 3. Send a signal (payment confirmed)
  signaled = client.signal_workflow(key, 'payment-confirmed', '{"amount": 99.99}')
  puts "3. Signal sent: #{signaled}"

  # 4. Query the workflow state
  puts '4. Querying workflow state...'
  current_status = client.get_status(key)
  puts "   Current status: #{current_status}"

  # 5. Complete the workflow
  puts '5. Completing workflow...'
  # client.complete_workflow(key, '{"result": "order shipped"}')

  puts
  puts '=== Basic workflow example finished! ==='
ensure
  client.close
end
