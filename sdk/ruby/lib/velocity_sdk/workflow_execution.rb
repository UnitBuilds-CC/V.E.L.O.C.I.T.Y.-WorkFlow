# frozen_string_literal: true

module VelocitySdk
  # Immutable handle to a running or completed workflow execution.
  #
  # Returned by {VelocityClient#start_workflow} and provides access to
  # the workflow's key, type, namespace, and current status.
  #
  # @example
  #   exec = client.start_workflow("my-workflow")
  #   puts exec.key          # => 12345
  #   puts exec.running?     # => true
  #   exec.status = "completed"
  #   puts exec.terminal?    # => true
  class WorkflowExecution
    # @return [Integer] Unique workflow key assigned by the engine.
    attr_reader :key

    # @return [String] Workflow type name.
    attr_reader :workflow_type

    # @return [String] Namespace the workflow runs in.
    attr_reader :namespace

    # @return [String] Current status (running, completed, failed, etc.).
    attr_accessor :status

    # @return [String, nil] Result payload (set when completed).
    attr_reader :result

    # @return [Integer] Unix timestamp (ms) when the workflow was started.
    attr_reader :started_at

    # Terminal status codes.
    TERMINAL_STATUSES = %w[completed failed canceled terminated].freeze

    # @param key [Integer] Workflow key.
    # @param workflow_type [String] Workflow type name.
    # @param namespace [String] Namespace.
    # @param status [String] Current status.
    # @param result [String, nil] Result payload.
    # @param started_at [Integer] Start timestamp in milliseconds.
    def initialize(key:, workflow_type:, namespace: 'default', status: 'running', result: nil, started_at: nil)
      @key = key
      @workflow_type = workflow_type
      @namespace = namespace
      @status = status
      @result = result
      @started_at = started_at || (Time.now.to_f * 1000).to_i
    end

    # Whether the workflow is still running.
    # @return [Boolean]
    def running?
      @status == 'running'
    end

    # Whether the workflow has reached a terminal state.
    # @return [Boolean]
    def terminal?
      TERMINAL_STATUSES.include?(@status)
    end

    # Convert to a Hash representation.
    # @return [Hash]
    def to_h
      {
        key: @key,
        workflow_type: @workflow_type,
        namespace: @namespace,
        status: @status,
        result: @result,
        started_at: @started_at,
      }
    end

    # @return [String]
    def inspect
      "#<VelocitySdk::WorkflowExecution key=#{@key} type=#{@workflow_type} status=#{@status}>"
    end
  end
end
