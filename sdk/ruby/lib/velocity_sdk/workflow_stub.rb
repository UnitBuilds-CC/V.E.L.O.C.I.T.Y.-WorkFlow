# frozen_string_literal: true

module VelocitySdk
  # Typed workflow execution stub.
  #
  # Provides a convenient interface for starting, signaling, querying,
  # and waiting for workflow results with automatic payload encoding/decoding.
  #
  # @example
  #   stub = WorkflowStub.new(client, 'order-processing')
  #     .namespace('default')
  #     .task_queue('orders')
  #     .codec(JsonCodec.new)
  #
  #   stub.start(order_id: '12345')
  #   stub.signal('approve', approved: true)
  #   result = stub.result
  class WorkflowStub
    # @param client [VelocityClient] The Velocity client
    # @param workflow_type [String] The workflow type name
    def initialize(client, workflow_type)
      @client = client
      @workflow_type = workflow_type
      @namespace = 'default'
      @task_queue = 'default'
      @codec = JsonCodec.new
      @handle = nil
    end

    # Set the namespace. Returns self for chaining.
    # @param ns [String] Namespace name
    # @return [self]
    def namespace(ns)
      @namespace = ns
      self
    end

    # Set the task queue. Returns self for chaining.
    # @param tq [String] Task queue name
    # @return [self]
    def task_queue(tq)
      @task_queue = tq
      self
    end

    # Set the payload codec. Returns self for chaining.
    # @param c [PayloadCodec] Codec instance
    # @return [self]
    def codec(c)
      @codec = c
      self
    end

    # Start workflow execution.
    # @param input [Object, nil] Input data (will be encoded via codec)
    # @return [self]
    def start(input = nil)
      payload = input ? @codec.encode(input) : ''
      @handle = @client.start_workflow(
        @workflow_type,
        namespace: @namespace,
        task_queue: @task_queue,
        input: payload
      )
      self
    end

    # Send a signal to the workflow.
    # @param signal_name [String] Name of the signal
    # @param data [Object, nil] Signal payload (will be encoded)
    def signal(signal_name, data = nil)
      ensure_started!
      payload = data ? @codec.encode(data) : ''
      @client.signal_workflow(@handle[:workflow_key], signal_name, payload)
    end

    # Query the workflow state.
    # @param query_type [String] Type of query
    # @param args [Object, nil] Query arguments (will be encoded)
    # @return [Object] Decoded query result
    def query(query_type, args = nil)
      ensure_started!
      payload = args ? @codec.encode(args) : ''
      result = @client.query_workflow(@handle[:workflow_key], query_type, payload)
      result && !result.empty? ? @codec.decode(result) : nil
    end

    # Wait for workflow completion and return the result.
    # @return [Object] Decoded workflow result
    def result
      ensure_started!
      result_data = @client.wait_for_completion(@handle[:workflow_key])
      result_data && !result_data.empty? ? @codec.decode(result_data) : nil
    end

    # Cancel the workflow.
    def cancel
      ensure_started!
      @client.cancel_workflow(@handle[:workflow_key])
    end

    # Terminate the workflow.
    # @param reason [String] Termination reason
    def terminate(reason = '')
      ensure_started!
      @client.terminate_workflow(@handle[:workflow_key], reason: reason)
    end

    # Get the workflow key (nil if not started).
    # @return [Integer, nil]
    def workflow_key
      @handle ? @handle[:workflow_key] : nil
    end

    # Get the underlying workflow handle.
    # @return [Hash, nil]
    def handle
      @handle
    end

    private

    def ensure_started!
      raise RuntimeError, 'Workflow not started. Call start() first.' unless @handle
    end
  end
end
