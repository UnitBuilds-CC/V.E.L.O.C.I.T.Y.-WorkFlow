# frozen_string_literal: true

# Example: Simple task worker using the VELOCITY-WorkFlow Ruby SDK.
#
# Demonstrates:
#   - Worker registration with a task queue
#   - Polling for tasks in a loop
#   - Executing task logic via registered handlers
#   - Error handling
#   - Signal handling for graceful shutdown (SIGINT / SIGTERM)
#
# Prerequisites:
#   1. Start the VELOCITY-WorkFlow server:
#      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
#
#   2. Install dependencies:
#      cd VELOCITY-WorkFlow/sdk/ruby && bundle install
#
#   3. Run this worker:
#      ruby examples/simple_worker.rb

require_relative '../lib/velocity_sdk'

# ── Configuration ────────────────────────────────────────────────────────

SERVER_ADDR = 'localhost:50051'
TASK_QUEUE  = 'orders'
POLL_INTERVAL_SEC = 1.0

# ── Graceful shutdown ────────────────────────────────────────────────────

$shutdown_requested = false

%w[INT TERM].each do |sig|
  trap(sig) do
    puts "[worker] Received SIG#{sig} — shutting down gracefully..."
    $shutdown_requested = true
  end
end

# ── Task handlers ────────────────────────────────────────────────────────

def process_order(task)
  input = JSON.parse(task['input'] || '{}')
  order_id = input['order_id'] || 'unknown'
  puts "[worker] Processing order #{order_id}"
  # Simulate work
  sleep(0.05)
  { 'status' => 'shipped', 'order_id' => order_id }
end

HANDLERS = {
  'order-processing' => method(:process_order),
}.freeze

# ── Worker loop ──────────────────────────────────────────────────────────

puts "[worker] Starting VELOCITY-WorkFlow Ruby worker"
puts "[worker] Server: #{SERVER_ADDR} | Queue: #{TASK_QUEUE}"

client = VelocitySdk::VelocityClient.new(target: SERVER_ADDR)

begin
  puts "[worker] Registered on task queue '#{TASK_QUEUE}'"
  puts "[worker] Polling for tasks... (Ctrl+C to stop)"

  while !$shutdown_requested
    begin
      # Poll for a task from the server
      task = client.poll_task(TASK_QUEUE, timeout_ms: 2000)

      if task.nil?
        sleep(POLL_INTERVAL_SEC)
        next
      end

      task_type = task['workflow_type'] || 'unknown'
      handler = HANDLERS[task_type]

      if handler.nil?
        puts "[worker] No handler for task type '#{task_type}' — skipping"
        client.fail_task(task['workflow_key'], "No handler for #{task_type}")
        next
      end

      # Execute the task
      result = handler.call(task)
      client.complete_workflow(task['workflow_key'], JSON.generate(result))
      puts "[worker] Task '#{task_type}' completed successfully"

    rescue VelocitySdk::VelocityError => e
      puts "[worker] Velocity error: #{e.message}"
      sleep(POLL_INTERVAL_SEC)

    rescue StandardError => e
      puts "[worker] Unexpected error: #{e.message}"
      sleep(POLL_INTERVAL_SEC)
    end
  end
ensure
  client.close
  puts '[worker] Shut down cleanly'
end
