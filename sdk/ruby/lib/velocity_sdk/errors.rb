# frozen_string_literal: true

module VelocitySdk
  # ─── Error hierarchy ─────────────────────────────────────────────────────────
  #
  # Error codes are consistent across all SDKs (Python, Go, TypeScript, Java, Rust, PHP).
  # Each error carries a numeric code, a human-readable message, and a retryable flag.

  # Base error for all VELOCITY-WorkFlow SDK operations.
  class VelocityError < StandardError
    # @return [Integer] Numeric error code matching other SDKs.
    attr_reader :error_code

    # @return [Boolean] Whether the operation can be retried.
    attr_reader :retryable

    # @return [Hash] Optional structured details.
    attr_reader :details

    # @param message [String] Human-readable error message.
    # @param error_code [Integer] Numeric error code.
    # @param retryable [Boolean] Whether the operation is retryable.
    # @param details [Hash] Additional context.
    def initialize(message, error_code: 0, retryable: false, details: {})
      super(message)
      @error_code = error_code
      @retryable = retryable
      @details = details
    end

    def to_s
      retry_str = @retryable ? ' (retryable)' : ''
      "VelocityError[#{@error_code}]: #{message}#{retry_str}"
    end
  end

  # Raised when a workflow does not exist (error code 1).
  class WorkflowNotFoundError < VelocityError
    # @return [Integer] The workflow key that was not found.
    attr_reader :workflow_key

    def initialize(workflow_key, message = nil)
      @workflow_key = workflow_key
      msg = message || "Workflow not found: #{workflow_key}"
      super(msg, error_code: 1, retryable: false, details: { workflow_key: workflow_key })
    end
  end

  # Raised when attempting to modify a completed workflow (error code 2).
  class WorkflowAlreadyCompletedError < VelocityError
    # @return [Integer] The workflow key that is already completed.
    attr_reader :workflow_key

    def initialize(workflow_key, message = nil)
      @workflow_key = workflow_key
      msg = message || "Workflow already completed: #{workflow_key}"
      super(msg, error_code: 2, retryable: false, details: { workflow_key: workflow_key })
    end
  end

  # Raised when connection to the server fails (error code 3).
  class ConnectionError < VelocityError
    # @return [String] The target address that failed.
    attr_reader :target

    def initialize(target, message = nil)
      @target = target
      msg = message || "Failed to connect to #{target}"
      super(msg, error_code: 3, retryable: true, details: { target: target })
    end
  end

  # Raised when an operation times out (error code 4).
  class TimeoutError < VelocityError
    # @return [String] The operation that timed out.
    attr_reader :operation
    # @return [Integer] Timeout in milliseconds.
    attr_reader :timeout_ms

    def initialize(operation, timeout_ms, message = nil)
      @operation = operation
      @timeout_ms = timeout_ms
      msg = message || "Operation '#{operation}' timed out after #{timeout_ms}ms"
      super(msg, error_code: 4, retryable: true, details: { operation: operation, timeout_ms: timeout_ms })
    end
  end

  # Raised when rate limit is exceeded (error code 5).
  class RateLimitError < VelocityError
    # @return [Integer] Milliseconds to wait before retrying.
    attr_reader :retry_after_ms

    def initialize(retry_after_ms: 0, message: nil)
      @retry_after_ms = retry_after_ms
      msg = message || 'Rate limit exceeded'
      super(msg, error_code: 5, retryable: true, details: { retry_after_ms: retry_after_ms })
    end
  end

  # Raised when authentication fails (error code 6).
  class AuthenticationError < VelocityError
    def initialize(message = nil)
      msg = message || 'Authentication failed'
      super(msg, error_code: 6, retryable: false)
    end
  end

  # Raised for internal server errors (error code 7).
  class InternalError < VelocityError
    def initialize(message = nil)
      msg = message || 'Internal server error'
      super(msg, error_code: 7, retryable: true)
    end
  end
end
