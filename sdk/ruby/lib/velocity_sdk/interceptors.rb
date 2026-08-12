# frozen_string_literal: true

module VelocitySdk
  # ─── Interceptor framework ───────────────────────────────────────────────────
  #
  # Interceptors implement a middleware pattern for workflow and activity
  # lifecycle hooks. They can be chained to compose logging, metrics, tracing,
  # and custom logic.

  # Module that defines the workflow interceptor interface.
  # Include this module and implement the callback methods.
  module WorkflowInterceptor
    # Called before a workflow starts.
    # @param workflow_type [String] Workflow type name.
    # @param workflow_key [Integer] Engine-assigned workflow key.
    def on_start(workflow_type, workflow_key); end

    # Called after a workflow completes successfully.
    # @param workflow_key [Integer] Workflow key.
    # @param result [String] Result payload.
    def on_complete(workflow_key, result); end

    # Called when a workflow fails.
    # @param workflow_key [Integer] Workflow key.
    # @param error [Exception] The error that caused the failure.
    def on_fail(workflow_key, error); end

    # Called when a workflow receives a signal.
    # @param workflow_key [Integer] Workflow key.
    # @param signal_name [String] Name of the signal.
    def on_signal(workflow_key, signal_name); end
  end

  # Module that defines the activity interceptor interface.
  module ActivityInterceptor
    # Called before an activity executes.
    # @param activity_type [String] Activity type name.
    # @param activity_id [String] Activity identifier.
    def on_execute(activity_type, activity_id); end

    # Called after an activity completes.
    # @param activity_id [String] Activity identifier.
    # @param result [String] Result payload.
    def on_activity_complete(activity_id, result); end

    # Called when an activity fails.
    # @param activity_id [String] Activity identifier.
    # @param error [Exception] The error that caused the failure.
    def on_activity_fail(activity_id, error); end
  end

  # Logs workflow and activity lifecycle events to $stderr.
  class LoggingInterceptor
    include WorkflowInterceptor
    include ActivityInterceptor

    # @param prefix [String] Log message prefix.
    def initialize(prefix = '[VELOCITY]')
      @prefix = prefix
    end

    def on_start(workflow_type, workflow_key)
      $stderr.puts "#{@prefix} Workflow started: type=#{workflow_type}, key=#{workflow_key}"
    end

    def on_complete(workflow_key, _result)
      $stderr.puts "#{@prefix} Workflow completed: key=#{workflow_key}"
    end

    def on_fail(workflow_key, error)
      $stderr.puts "#{@prefix} Workflow failed: key=#{workflow_key}, error=#{error.message}"
    end

    def on_signal(workflow_key, signal_name)
      $stderr.puts "#{@prefix} Workflow signal: key=#{workflow_key}, signal=#{signal_name}"
    end

    def on_execute(activity_type, activity_id)
      $stderr.puts "#{@prefix} Activity executing: type=#{activity_type}, id=#{activity_id}"
    end

    def on_activity_complete(activity_id, _result)
      $stderr.puts "#{@prefix} Activity completed: id=#{activity_id}"
    end

    def on_activity_fail(activity_id, error)
      $stderr.puts "#{@prefix} Activity failed: id=#{activity_id}, error=#{error.message}"
    end
  end

  # Tracks workflow and activity metrics (counts).
  class MetricsInterceptor
    include WorkflowInterceptor
    include ActivityInterceptor

    attr_reader :workflow_starts, :workflow_completions, :workflow_failures
    attr_reader :activity_executions, :activity_completions, :activity_failures

    def initialize
      @workflow_starts = 0
      @workflow_completions = 0
      @workflow_failures = 0
      @activity_executions = 0
      @activity_completions = 0
      @activity_failures = 0
      @start_times = {}
    end

    def on_start(_workflow_type, workflow_key)
      @workflow_starts += 1
      @start_times[workflow_key] = Time.now
    end

    def on_complete(workflow_key, _result)
      @workflow_completions += 1
      @start_times.delete(workflow_key)
    end

    def on_fail(workflow_key, _error)
      @workflow_failures += 1
      @start_times.delete(workflow_key)
    end

    def on_signal(_workflow_key, _signal_name)
      # Signals don't affect metrics counters.
    end

    def on_execute(_activity_type, _activity_id)
      @activity_executions += 1
    end

    def on_activity_complete(_activity_id, _result)
      @activity_completions += 1
    end

    def on_activity_fail(_activity_id, _error)
      @activity_failures += 1
    end

    # Return a snapshot of current metrics.
    # @return [Hash] Metrics hash.
    def snapshot
      {
        workflow_starts: @workflow_starts,
        workflow_completions: @workflow_completions,
        workflow_failures: @workflow_failures,
        activity_executions: @activity_executions,
        activity_completions: @activity_completions,
        activity_failures: @activity_failures,
      }
    end
  end

  # Chain of interceptors invoked in insertion order.
  class InterceptorChain
    def initialize
      @interceptors = []
    end

    # Add an interceptor to the chain.
    # @param interceptor [Object] An object that includes WorkflowInterceptor and/or ActivityInterceptor.
    # @return [self]
    def add(interceptor)
      @interceptors << interceptor
      self
    end

    # Number of interceptors in the chain.
    def size
      @interceptors.size
    end

    # Invoke on_start for all workflow interceptors.
    def invoke_start(workflow_type, workflow_key)
      @interceptors.each do |i|
        i.on_start(workflow_type, workflow_key) if i.respond_to?(:on_start)
      end
    end

    # Invoke on_complete for all workflow interceptors.
    def invoke_complete(workflow_key, result)
      @interceptors.each do |i|
        i.on_complete(workflow_key, result) if i.respond_to?(:on_complete)
      end
    end

    # Invoke on_fail for all workflow interceptors.
    def invoke_fail(workflow_key, error)
      @interceptors.each do |i|
        i.on_fail(workflow_key, error) if i.respond_to?(:on_fail)
      end
    end

    # Invoke on_signal for all workflow interceptors.
    def invoke_signal(workflow_key, signal_name)
      @interceptors.each do |i|
        i.on_signal(workflow_key, signal_name) if i.respond_to?(:on_signal)
      end
    end

    # Invoke on_execute for all activity interceptors.
    def invoke_activity_execute(activity_type, activity_id)
      @interceptors.each do |i|
        i.on_execute(activity_type, activity_id) if i.respond_to?(:on_execute)
      end
    end
  end
end
