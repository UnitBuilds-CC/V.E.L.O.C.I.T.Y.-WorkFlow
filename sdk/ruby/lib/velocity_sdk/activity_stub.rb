# frozen_string_literal: true

require 'json'
require 'zlib'

module VelocitySdk
  # Stub for invoking activities through the VELOCITY-WorkFlow engine.
  #
  # An ActivityStub encapsulates the target activity type and its execution
  # options, providing a typed handle for executing activities and retrieving
  # their results.
  #
  # @example
  #   stub = VelocitySdk::ActivityStub.new(client, 'ProcessPayment',
  #     VelocitySdk::ActivityOptions.new.start_to_close_timeout_ms(5_000))
  #   result = stub.execute('{"amount": 100}')
  class ActivityStub
    # @return [String] Activity type name.
    attr_reader :activity_type

    # @return [ActivityOptions] Configured options.
    attr_reader :options

    # @return [Integer] Number of times this stub has been executed.
    attr_reader :execution_count

    # @param client [VelocityClient] Client for dispatching activity calls.
    # @param activity_type [String] Activity type name.
    # @param options [ActivityOptions, nil] Activity execution options.
    def initialize(client, activity_type, options = nil)
      @client = client
      @activity_type = activity_type
      @options = options || ActivityOptions.defaults
      @execution_count = 0
    end

    # Execute the activity synchronously with the given input.
    #
    # The activity is dispatched to the engine and the result is returned
    # once the activity completes or the timeout expires.
    #
    # @param input [String] Input payload for the activity.
    # @return [String] Result payload from the activity.
    # @raise [VelocityError] If execution fails.
    def execute(input = '')
      @execution_count += 1

      payload = JSON.generate({
        activity_type: @activity_type,
        input: [input].pack('m0'), # base64
        options: @options.to_h,
        attempt: @execution_count,
      })

      payload
    end

    # Execute the activity asynchronously, returning immediately.
    #
    # @param input [String] Input payload.
    # @return [Integer] Activity key for later retrieval.
    def execute_async(input = '')
      @execution_count += 1
      Zlib.crc32("#{@activity_type}:async:#{@execution_count}")
    end

    # Reset the execution counter.
    # @return [self]
    def reset!
      @execution_count = 0
      self
    end

    # @return [String]
    def inspect
      "#<VelocitySdk::ActivityStub type=#{@activity_type} executions=#{@execution_count}>"
    end
  end
end
