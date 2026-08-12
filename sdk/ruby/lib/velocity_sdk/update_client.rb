# frozen_string_literal: true

# Workflow Update API — synchronous workflow mutation.
#
# Unlike signals (fire-and-forget), updates provide:
# - Synchronous request/response semantics
# - Wait policies (Accepted, Completed, Admitted)
# - Validation before execution
# - Named update handlers registered by workflows
#
# Usage:
#   client = VelocitySdk::UpdateClient.new('localhost:50051')
#   client.register_handler('setAmount', ->(args) { args })
#   result = client.execute_update(workflow_key: 42, update_name: 'setAmount', args: { amount: 100 })

module VelocitySdk
  # Status of a workflow update.
  module UpdateStatus
    ADMITTED = 0
    ACCEPTED = 1
    COMPLETED = 2
    REJECTED = 3
  end

  # How long to wait for an update to complete.
  module UpdateWaitPolicy
    ADMITTED = 0
    ACCEPTED = 1
    COMPLETED = 2
  end

  # Request to execute a workflow update.
  UpdateRequest = Struct.new(:workflow_key, :update_id, :update_name, :args, :wait_policy, keyword_init: true)

  # Result of a workflow update execution.
  UpdateResult = Struct.new(:update_id, :status, :result, :error, :duration_ms, keyword_init: true)

  # Handler for a named update.
  UpdateHandler = Struct.new(:name, :handler, :validator, keyword_init: true)

  # Client for executing workflow updates.
  class UpdateClient
    def initialize(server_address = 'localhost:50051')
      @server_address = server_address
      @handlers = {}
      @pending = {}
      @mutex = Mutex.new
    end

    # Register a named update handler.
    #
    # @param name [String] handler name
    # @param handler [Proc] handler function
    # @param validator [Proc, nil] optional validation function
    def register_handler(name, handler, validator: nil)
      @handlers[name] = UpdateHandler.new(name: name, handler: handler, validator: validator)
    end

    # Execute a workflow update.
    #
    # @param workflow_key [Integer] target workflow key
    # @param update_name [String] name of the registered update handler
    # @param args [Object] arguments to pass to the handler
    # @param wait_policy [Integer] how long to wait for completion
    # @param update_id [String, nil] optional update ID
    # @return [UpdateResult]
    def execute_update(workflow_key:, update_name:, args: nil, wait_policy: UpdateWaitPolicy::COMPLETED, update_id: nil)
      uid = update_id || "update-#{workflow_key}-#{(Time.now.to_f * 1000).to_i}"
      start = Time.now

      handler = @handlers[update_name]
      if handler.nil?
        result = UpdateResult.new(
          update_id: uid,
          status: UpdateStatus::REJECTED,
          error: "No handler registered for update '#{update_name}'",
          duration_ms: elapsed_ms(start)
        )
        @mutex.synchronize { @pending[uid] = result }
        return result
      end

      if handler.validator && !handler.validator.call(args)
        result = UpdateResult.new(
          update_id: uid,
          status: UpdateStatus::REJECTED,
          error: 'Update validation failed',
          duration_ms: elapsed_ms(start)
        )
        @mutex.synchronize { @pending[uid] = result }
        return result
      end

      begin
        value = handler.handler.call(args)
        result = UpdateResult.new(
          update_id: uid,
          status: UpdateStatus::COMPLETED,
          result: value,
          duration_ms: elapsed_ms(start)
        )
      rescue StandardError => e
        result = UpdateResult.new(
          update_id: uid,
          status: UpdateStatus::REJECTED,
          error: e.message,
          duration_ms: elapsed_ms(start)
        )
      end

      @mutex.synchronize { @pending[uid] = result }
      result
    end

    # Get the result of a previously executed update.
    def get_update_result(update_id)
      @mutex.synchronize { @pending[update_id] }
    end

    # List registered update handler names.
    def list_handlers
      @handlers.keys
    end

    # List pending update IDs.
    def list_pending
      @mutex.synchronize { @pending.keys }
    end

    private

    def elapsed_ms(start)
      ((Time.now - start) * 1000.0).round(2)
    end
  end
end
