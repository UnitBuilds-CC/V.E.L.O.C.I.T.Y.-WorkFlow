# frozen_string_literal: true

module VelocitySdk
  # Builder for workflow execution options.
  #
  # Provides a fluent interface for configuring workflow parameters such as
  # namespace, task queue, timeouts, retry policy, and execution steps.
  #
  # @example
  #   options = VelocitySdk::WorkflowOptions.new
  #     .namespace('production')
  #     .task_queue('high-priority')
  #     .total_steps(10)
  #     .execution_timeout_ms(30_000)
  #     .retry_policy(max_attempts: 3)
  class WorkflowOptions
    # @return [String] Namespace to run in.
    attr_reader :namespace

    # @return [String] Task queue for worker dispatch.
    attr_reader :task_queue

    # @return [Integer] Number of execution steps.
    attr_reader :total_steps

    # @return [Integer] Execution timeout in milliseconds.
    attr_reader :execution_timeout_ms

    # @return [Hash] Retry policy configuration.
    attr_reader :retry_policy

    # @return [String] Explicit workflow ID (empty = server-assigned).
    attr_reader :workflow_id

    # @return [Hash] Search attributes for visibility queries.
    attr_reader :search_attributes

    # @return [String, nil] Memo attached to the workflow.
    attr_reader :memo

    # Create a new options builder with defaults.
    def initialize
      @namespace = 'default'
      @task_queue = 'default'
      @total_steps = 1
      @execution_timeout_ms = 60_000
      @retry_policy = {}
      @workflow_id = ''
      @search_attributes = {}
      @memo = nil
    end

    # Create options with all defaults.
    # @return [WorkflowOptions]
    def self.defaults
      new
    end

    # Set the namespace.
    # @param value [String]
    # @return [self]
    def namespace(value)
      @namespace = value
      self
    end

    # Set the task queue.
    # @param value [String]
    # @return [self]
    def task_queue(value)
      @task_queue = value
      self
    end

    # Set the total number of execution steps.
    # @param value [Integer]
    # @return [self]
    def total_steps(value)
      @total_steps = [1, value].max
      self
    end

    # Set the execution timeout in milliseconds.
    # @param value [Integer]
    # @return [self]
    def execution_timeout_ms(value)
      @execution_timeout_ms = [0, value].max
      self
    end

    # Set the retry policy.
    # @param value [Hash]
    # @return [self]
    def retry_policy(value)
      @retry_policy = value
      self
    end

    # Set an explicit workflow ID.
    # @param value [String]
    # @return [self]
    def workflow_id(value)
      @workflow_id = value
      self
    end

    # Set search attributes.
    # @param value [Hash]
    # @return [self]
    def search_attributes(value)
      @search_attributes = value
      self
    end

    # Set a memo.
    # @param value [String]
    # @return [self]
    def memo(value)
      @memo = value
      self
    end

    # Convert to a Hash for serialisation.
    # @return [Hash]
    def to_h
      {
        namespace: @namespace,
        task_queue: @task_queue,
        total_steps: @total_steps,
        execution_timeout_ms: @execution_timeout_ms,
        retry_policy: @retry_policy,
        workflow_id: @workflow_id,
        search_attributes: @search_attributes,
        memo: @memo,
      }
    end

    # @return [String]
    def inspect
      "#<VelocitySdk::WorkflowOptions ns=#{@namespace} tq=#{@task_queue} steps=#{@total_steps}>"
    end
  end
end
