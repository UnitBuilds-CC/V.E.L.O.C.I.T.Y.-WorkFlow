# frozen_string_literal: true

module VelocitySdk
  # Configuration for retry behavior with exponential backoff.
  #
  # @example
  #   policy = RetryPolicy.new(
  #     max_attempts: 5,
  #     initial_interval_ms: 100,
  #     backoff_coefficient: 2.0,
  #     max_interval_ms: 10_000,
  #     jitter: true,
  #   )
  #
  #   result = retry_with_policy(policy) { fetch_remote_data }
  class RetryPolicy
    attr_reader :max_attempts, :initial_interval_ms, :backoff_coefficient,
                :max_interval_ms, :jitter, :retryable_exceptions

    # @param max_attempts [Integer] Maximum number of attempts (>= 1)
    # @param initial_interval_ms [Numeric] Initial backoff interval in ms
    # @param backoff_coefficient [Numeric] Backoff multiplier (>= 1.0)
    # @param max_interval_ms [Numeric] Maximum backoff interval in ms
    # @param jitter [Boolean] Whether to add random jitter
    # @param retryable_exceptions [Array<Class>] Exception classes to retry on
    def initialize(
      max_attempts: 3,
      initial_interval_ms: 100,
      backoff_coefficient: 2.0,
      max_interval_ms: 60_000,
      jitter: true,
      retryable_exceptions: []
    )
      @max_attempts = max_attempts
      @initial_interval_ms = initial_interval_ms
      @backoff_coefficient = backoff_coefficient
      @max_interval_ms = max_interval_ms
      @jitter = jitter
      @retryable_exceptions = retryable_exceptions
      validate!
    end

    # Create a default retry policy.
    def self.defaults
      new
    end

    # Validate the policy configuration.
    # @raise [ArgumentError] if configuration is invalid
    def validate!
      raise ArgumentError, 'max_attempts must be >= 1' if @max_attempts < 1
      raise ArgumentError, 'initial_interval_ms must be > 0' if @initial_interval_ms <= 0
      raise ArgumentError, 'backoff_coefficient must be >= 1.0' if @backoff_coefficient < 1.0
      raise ArgumentError, 'max_interval_ms must be >= initial_interval_ms' if @max_interval_ms < @initial_interval_ms
    end

    # Calculate backoff duration for a given attempt (0-based).
    # @param attempt [Integer] Zero-based attempt index
    # @return [Numeric] Backoff duration in milliseconds
    def calculate_backoff(attempt)
      interval = @initial_interval_ms * (@backoff_coefficient**attempt)
      interval = [interval, @max_interval_ms].min

      if @jitter
        interval = rand * interval
      end

      interval
    end

    # Check if an exception is retryable.
    # @param error [Exception] The exception to check
    # @return [Boolean]
    def retryable?(error)
      return true if @retryable_exceptions.empty?
      @retryable_exceptions.any? { |klass| error.is_a?(klass) }
    end
  end

  # Execute a block with retry logic and exponential backoff.
  #
  # @param policy [RetryPolicy] Retry configuration
  # @yield The operation to execute
  # @return [Object] Result of the block
  # @raise [Exception] The last exception if all retries fail
  def self.retry_with_policy(policy, &block)
    policy.validate!

    last_error = nil

    policy.max_attempts.times do |attempt|
      begin
        return block.call
      rescue => e
        last_error = e

        raise e unless policy.retryable?(e)

        if attempt < policy.max_attempts - 1
          backoff_ms = policy.calculate_backoff(attempt)
          sleep(backoff_ms / 1000.0)
        end
      end
    end

    raise last_error
  end

  # Execute a block with default retry options.
  #
  # @param max_attempts [Integer] Maximum number of attempts
  # @param initial_interval_ms [Numeric] Initial backoff interval in ms
  # @param backoff_coefficient [Numeric] Backoff multiplier
  # @param max_interval_ms [Numeric] Maximum backoff interval in ms
  # @param jitter [Boolean] Whether to add random jitter
  # @yield The operation to execute
  # @return [Object] Result of the block
  def self.retry_with_backoff(
    max_attempts: 3,
    initial_interval_ms: 100,
    backoff_coefficient: 2.0,
    max_interval_ms: 60_000,
    jitter: true,
    &block
  )
    policy = RetryPolicy.new(
      max_attempts: max_attempts,
      initial_interval_ms: initial_interval_ms,
      backoff_coefficient: backoff_coefficient,
      max_interval_ms: max_interval_ms,
      jitter: jitter,
    )
    retry_with_policy(policy, &block)
  end
end
