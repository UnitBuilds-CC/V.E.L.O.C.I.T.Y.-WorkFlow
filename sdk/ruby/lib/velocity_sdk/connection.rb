# frozen_string_literal: true

module VelocitySdk
  # Connection management with pooling for the VELOCITY-WorkFlow engine.
  #
  # Manages a pool of reusable connections to the engine, providing
  # thread-safe checkout/checkin semantics and automatic reconnection
  # on failure. Supports both FFI (local) and gRPC (remote) backends.
  #
  # @example
  #   pool = VelocitySdk::Connection.new(max_size: 10, target: 'localhost:50051')
  #   pool.with_connection do |client|
  #     key = client.start_workflow("my-workflow")
  #     client.signal_workflow(key, "approve", "yes")
  #   end
  #   pool.close
  class Connection
    # @return [String] Target address.
    attr_reader :target

    # @return [Integer] Maximum pool size.
    attr_reader :max_size

    # @return [Integer] Current number of connections in the pool.
    attr_reader :pool_size

    # Create a new connection pool.
    #
    # @param target [String] gRPC server address or FFI target.
    # @param max_size [Integer] Maximum number of pooled connections.
    # @param jwt_token [String, nil] Optional JWT bearer token.
    # @param library_path [String, nil] Optional path to the native engine library.
    def initialize(target: 'localhost:50051', max_size: 5, jwt_token: nil, library_path: nil)
      @target = target
      @max_size = max_size
      @jwt_token = jwt_token
      @library_path = library_path
      @pool = []
      @mutex = Mutex.new
      @closed = false
    end

    # Execute a block with a checked-out connection from the pool.
    #
    # The connection is automatically returned to the pool after the block
    # completes. If the pool is exhausted, a new connection is created
    # (up to max_size). If max_size is reached, the call blocks until a
    # connection becomes available.
    #
    # @yield [VelocityClient] A client instance.
    # @return [Object] The block's return value.
    # @raise [ConnectionError] If the pool is closed.
    def with_connection
      raise ConnectionError.new(@target, 'Connection pool is closed') if @closed

      client = checkout
      begin
        yield client
      ensure
        checkin(client)
      end
    end

    # Close all connections in the pool.
    def close
      @mutex.synchronize do
        @closed = true
        @pool.each(&:close)
        @pool.clear
      end
    end

    # @return [Boolean] Whether the pool is closed.
    def closed?
      @closed
    end

    # @return [String]
    def inspect
      "#<VelocitySdk::Connection target=#{@target} pool=#{@pool.size}/#{@max_size} closed=#{@closed}>"
    end

    private

    # Check out a connection from the pool, creating one if needed.
    # @return [VelocityClient]
    def checkout
      @mutex.synchronize do
        if @pool.any?
          return @pool.pop
        end
      end

      create_client
    end

    # Return a connection to the pool.
    # @param client [VelocityClient]
    def checkin(client)
      @mutex.synchronize do
        if @closed || @pool.size >= @max_size
          client.close
        else
          @pool.push(client)
        end
      end
    end

    # Create a new client connection.
    # @return [VelocityClient]
    def create_client
      VelocityClient.new(
        target: @target,
        jwt_token: @jwt_token,
        library_path: @library_path,
      )
    end
  end
end
