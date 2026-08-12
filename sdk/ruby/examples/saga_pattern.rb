# frozen_string_literal: true

# Example: Multi-step saga with compensation using the VELOCITY-WorkFlow Ruby SDK.
#
# Demonstrates:
#   - Defining a saga with compensable steps
#   - Executing steps in order
#   - Triggering compensation on failure
#   - Rolling back completed steps in reverse order
#
# Prerequisites:
#   1. Start the VELOCITY-WorkFlow server:
#      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
#   2. bundle install
#   3. ruby examples/saga_pattern.rb

require_relative '../lib/velocity_sdk'

# SagaStep defines a forward action and its compensation.
SagaStep = Struct.new(:name, :compensate, keyword_init: true)

STEPS = [
  SagaStep.new(name: 'reserve_inventory', compensate: 'release_inventory'),
  SagaStep.new(name: 'charge_payment',    compensate: 'refund_payment'),
  SagaStep.new(name: 'book_shipping',     compensate: 'cancel_shipping'),
  SagaStep.new(name: 'send_confirmation', compensate: 'send_cancellation_notice'),
].freeze

# Run the saga. If simulate_failure_at is set, the step at that index fails.
def run_saga(client, simulate_failure_at: nil)
  key = client.start_workflow(
    'order-saga',
    namespace: 'default',
    task_queue: 'orders',
    total_steps: STEPS.length,
  )
  puts "  Saga started: key=#{key}"

  completed_steps = []

  STEPS.each_with_index do |step, i|
    if simulate_failure_at && i == simulate_failure_at
      puts "\n   ✗ Step '#{step.name}' FAILED — triggering compensation"
      # Compensate in reverse order
      completed_steps.reverse_each do |prev|
        puts "   Compensating: #{prev.compensate}"
        client.signal_workflow(key, prev.compensate, '')
      end
      return false
    end

    puts "   Executing: #{step.name}"
    client.signal_workflow(key, step.name, '')
    completed_steps << step
  end

  puts '   ✓ All saga steps completed successfully'
  true
end

puts '=== VELOCITY-WorkFlow Ruby SDK — Saga Pattern ==='
puts

client = VelocitySdk::VelocityClient.new(target: 'localhost:50051')

begin
  # Scenario 1: Happy path
  puts 'Scenario 1: Happy path'
  run_saga(client)

  # Scenario 2: Payment step fails (index=1)
  puts "\nScenario 2: Payment step fails (index=1)"
  run_saga(client, simulate_failure_at: 1)
ensure
  client.close
end

puts "\n=== Saga examples finished! ==="
