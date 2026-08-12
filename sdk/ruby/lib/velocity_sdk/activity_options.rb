# frozen_string_literal: true

module VelocitySdk
  # Configuration options for activity execution.
  #
  # Controls timeouts, retry behaviour, and task queue routing for
  # individual activity invocations within a workflow.
  #
  # @example
  #   opts = VelocitySdk::ActivityOptions.new
  #     .start_to_close_timeout_ms(5_000)
  #     .retry_max_attempts(3)
  #     .task_queue('activity-workers')
  class ActivityOptions
    # @return [Integer] Start-to-close timeout in milliseconds.
    attr_reader :start_to_close_timeout_ms

    # @return [Integer] Schedule-to-close timeout in milliseconds.
    attr_reader :schedule_to_close_timeout_ms

    # @return [Integer] Heartbeat timeout in milliseconds (0 = disabled).
    attr_reader :heartbeat_timeout_ms

    # @return [String] Task queue for activity dispatch.
    attr_reader :task_queue

    # @return [Integer] Maximum number of retry attempts.
    attr_reader :retry_max_attempts

    # @return [Float] Retry backoff coefficient.
    attr_reader :retry_backoff_coefficient

    # @return [Integer] Initial retry interval in milliseconds.
    attr_reader :retry_initial_interval_ms

    # Create a new options builder with defaults.
    def initialize
      @start_to_close_timeout_ms = 10_000
      @schedule_to_close_timeout_ms = 60_000
      @heartbeat_timeout_ms = 0
      @task_queue = 'default'
      @retry_max_attempts = 1
      @retry_backoff_coefficient = 2.0
      @retry_initial_interval_ms = 100
    end

    # Create options with all defaults.
    # @return [ActivityOptions]
    def self.defaults
      new
    end

    # Set the start-to-close timeout in milliseconds.
    # @param value [Integer]
    # @return [self]
    def start_to_close_timeout_ms(value)
      @start_to_close_timeout_ms = [0, value].max
      self
    end

    # Set the schedule-to-close timeout in milliseconds.
    # @param value [Integer]
    # @return [self]
    def schedule_to_close_timeout_ms(value)
      @schedule_to_close_timeout_ms = [0, value].max
      self
    end

    # Set the heartbeat timeout in milliseconds.
    # @param value [Integer]
    # @return [self]
    def heartbeat_timeout_ms(value)
      @heartbeat_timeout_ms = [0, value].max
      self
    end

    # Set the task queue for activity dispatch.
    # @param value [String]
    # @return [self]
    def task_queue(value)
      @task_queue = value
      self
    end

    # Set the maximum number of retry attempts.
    # @param value [Integer]
    # @return [self]
    def retry_max_attempts(value)
      @retry_max_attempts = [1, value].max
      self
    end

    # Set the retry backoff coefficient.
    # @param value [Float]
    # @return [self]
    def retry_backoff_coefficient(value)
      @retry_backoff_coefficient = [1.0, value].max
      self
    end

    # Set the initial retry interval in milliseconds.
    # @param value [Integer]
    # @return [self]
    def retry_initial_interval_ms(value)
      @retry_initial_interval_ms = [0, value].max
      self
    end

    # Convert to a Hash for serialisation.
    # @return [Hash]
    def to_h
      {
        start_to_close_timeout_ms: @start_to_close_timeout_ms,
        schedule_to_close_timeout_ms: @schedule_to_close_timeout_ms,
        heartbeat_timeout_ms: @heartbeat_timeout_ms,
        task_queue: @task_queue,
        retry_max_attempts: @retry_max_attempts,
        retry_backoff_coefficient: @retry_backoff_coefficient,
        retry_initial_interval_ms: @retry_initial_interval_ms,
      }
    end

    # @return [String]
    def inspect
      "#<VelocitySdk::ActivityOptions tq=#{@task_queue} timeout=#{@start_to_close_timeout_ms}ms retries=#{@retry_max_attempts}>"
    end
  end
end
