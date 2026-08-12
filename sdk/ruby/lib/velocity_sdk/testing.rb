# frozen_string_literal: true

module VelocitySdk
  # ─── Testing utilities ───────────────────────────────────────────────────────
  #
  # Provides TestWorkflowEnvironment and MockClient for unit-testing
  # workflow logic without a running engine or server.

  # In-memory mock client that mirrors the VelocityClient API surface.
  #
  # Useful for unit tests that need to verify workflow interactions without
  # depending on the real engine or FFI library.
  class MockClient
    # @return [Hash<Integer, Hash>] Internal workflow state store.
    attr_reader :workflows

    # @return [Hash<Integer, Array<Hash>>] Signal store per workflow.
    attr_reader :signals

    def initialize
      @workflows = {}
      @signals = {}
      @next_key = 1
    end

    # Start a mock workflow.
    # @return [Integer] Workflow key.
    def start_workflow(workflow_type, namespace: 'default', task_queue: 'default', total_steps: 1, input: '')
      key = @next_key
      @next_key += 1

      @workflows[key] = {
        workflow_type: workflow_type,
        namespace: namespace,
        task_queue: task_queue,
        total_steps: total_steps,
        current_step: 0,
        status: WorkflowStatus::RUNNING,
        result: nil,
      }
      @signals[key] = []
      key
    end

    # Describe a mock workflow.
    # @return [Hash] Workflow description.
    # @raise [WorkflowNotFoundError]
    def describe_workflow(workflow_key)
      wf = @workflows[workflow_key]
      raise WorkflowNotFoundError.new(workflow_key) unless wf

      {
        workflow_key: workflow_key,
        status: wf[:status],
        current_step: wf[:current_step],
        total_steps: wf[:total_steps],
        namespace: wf[:namespace],
        result: wf[:result],
      }
    end

    # Signal a mock workflow.
    # @raise [WorkflowNotFoundError]
    def signal_workflow(workflow_key, signal_name, payload = '')
      raise WorkflowNotFoundError.new(workflow_key) unless @workflows.key?(workflow_key)

      @signals[workflow_key] << { signal_name: signal_name, payload: payload }
      true
    end

    # Complete a mock workflow.
    # @raise [WorkflowNotFoundError]
    # @raise [WorkflowAlreadyCompletedError]
    def complete_workflow(workflow_key, result = '')
      wf = @workflows[workflow_key]
      raise WorkflowNotFoundError.new(workflow_key) unless wf
      raise WorkflowAlreadyCompletedError.new(workflow_key) unless wf[:status] == WorkflowStatus::RUNNING

      wf[:status] = WorkflowStatus::COMPLETED
      wf[:result] = result
      true
    end

    # Fail a mock workflow.
    # @raise [WorkflowNotFoundError]
    # @raise [WorkflowAlreadyCompletedError]
    def fail_workflow(workflow_key, reason = '')
      wf = @workflows[workflow_key]
      raise WorkflowNotFoundError.new(workflow_key) unless wf
      raise WorkflowAlreadyCompletedError.new(workflow_key) unless wf[:status] == WorkflowStatus::RUNNING

      wf[:status] = WorkflowStatus::FAILED
      true
    end

    # Cancel a mock workflow.
    # @raise [WorkflowNotFoundError]
    def cancel_workflow(workflow_key)
      wf = @workflows[workflow_key]
      raise WorkflowNotFoundError.new(workflow_key) unless wf

      wf[:status] = WorkflowStatus::CANCELED
      true
    end

    # Get the status of a mock workflow.
    # @return [Integer] Status code.
    # @raise [WorkflowNotFoundError]
    def get_status(workflow_key)
      wf = @workflows[workflow_key]
      raise WorkflowNotFoundError.new(workflow_key) unless wf

      wf[:status]
    end

    # Get all signals received by a workflow.
    # @return [Array<Hash>]
    def get_signals(workflow_key)
      @signals[workflow_key] || []
    end

    # List all workflow keys.
    # @return [Array<Integer>]
    def list_workflows
      @workflows.keys
    end

    # Close the mock client (no-op).
    def close; end
  end

  # Isolated test environment wrapping a MockClient.
  #
  # Provides assertion helpers and time-skip support for deterministic tests.
  class TestWorkflowEnvironment
    # @return [MockClient] The underlying mock client.
    attr_reader :client

    def initialize
      @client = MockClient.new
      @time_offset_secs = 0
    end

    # Start a workflow in the test environment.
    # @return [Integer] Workflow key.
    def start_workflow(workflow_type, **kwargs)
      @client.start_workflow(workflow_type, **kwargs)
    end

    # Complete a workflow in the test environment.
    def complete_workflow(workflow_key, result = '')
      @client.complete_workflow(workflow_key, result)
    end

    # Signal a workflow in the test environment.
    def signal_workflow(workflow_key, signal_name, payload = '')
      @client.signal_workflow(workflow_key, signal_name, payload)
    end

    # Advance the simulated clock.
    # @param seconds [Integer]
    def time_skip(seconds)
      @time_offset_secs += seconds
    end

    # Current simulated time as UNIX epoch seconds.
    # @return [Integer]
    def current_time_secs
      Time.now.to_i + @time_offset_secs
    end

    # Assert that a workflow has completed.
    # @raise [RuntimeError] If the workflow is not completed.
    def assert_workflow_completed(workflow_key)
      desc = @client.describe_workflow(workflow_key)
      return if desc[:status] == WorkflowStatus::COMPLETED

      raise "Expected workflow #{workflow_key} to be completed, but status is #{WorkflowStatus.name_for(desc[:status])}"
    end

    # Assert that a workflow received a specific signal.
    # @raise [RuntimeError] If the signal was not received.
    def assert_signal_received(workflow_key, signal_name)
      names = @client.get_signals(workflow_key).map { |s| s[:signal_name] }
      return if names.include?(signal_name)

      raise "Expected signal '#{signal_name}' not found for workflow #{workflow_key}. Received: #{names.join(', ')}"
    end

    # Reset the environment to a clean state.
    def reset
      @client = MockClient.new
      @time_offset_secs = 0
    end
  end
end
