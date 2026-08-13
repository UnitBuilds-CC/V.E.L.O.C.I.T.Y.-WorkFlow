# frozen_string_literal: true

require 'ffi'
require 'zlib'

module VelocitySdk
  # Workflow execution status codes (matching the engine's WorkflowStatus enum).
  module WorkflowStatus
    VOID = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3
    CANCELED = 4
    TERMINATED = 5
    CONTINUED_AS_NEW = 6
    TIMED_OUT = 7

    STATUS_NAMES = {
      VOID => 'void',
      RUNNING => 'running',
      COMPLETED => 'completed',
      FAILED => 'failed',
      CANCELED => 'canceled',
      TERMINATED => 'terminated',
      CONTINUED_AS_NEW => 'continued_as_new',
      TIMED_OUT => 'timed_out',
    }.freeze

    def self.name_for(code)
      STATUS_NAMES.fetch(code, 'unknown')
    end
  end

  # FFI bindings to the native velocity_workflow_engine shared library.
  module EngineFfi
    extend FFI::Library

    # Attempt to load the native library. Returns nil if not available.
    def self.try_load(library_path = nil)
      candidates = if library_path
                     [library_path]
                   else
                     [
                       'velocity_workflow_engine.dll',
                       'libvelocity_workflow_engine.so',
                       'libvelocity_workflow_engine.dylib',
                     ].flat_map do |name|
                       [
                         File.join(__dir__, '..', '..', '..', 'velocity-workflow-engine', 'target', 'release', name),
                         File.join(__dir__, '..', '..', '..', 'velocity-workflow-engine', 'target', 'debug', name),
                         name,
                       ]
                     end
                   end

      candidates.each do |path|
        begin
          ffi_lib path
          return true
        rescue LoadError
          next
        end
      end
      false
    rescue LoadError
      false
    end

    # Only define FFI functions if the library was loaded.
    def self.setup_functions
      attach_function :velocity_engine_create, [], :pointer
      attach_function :velocity_engine_destroy, [:pointer], :int
      attach_function :velocity_engine_start_workflow,
                      [:pointer, :uint64, :uint64, :uint64, :uint64, :uint32, :pointer, :uint32], :uint64
      attach_function :velocity_engine_complete_step,
                      [:pointer, :uint64, :uint32, :pointer, :uint32], :int
      attach_function :velocity_engine_signal_workflow,
                      [:pointer, :uint64, :uint64, :pointer, :uint32], :void
      attach_function :velocity_engine_cancel_workflow, [:pointer, :uint64], :void
      attach_function :velocity_engine_get_status, [:pointer, :uint64], :int
    rescue FFI::NotFoundError
      # Functions not available; degrade gracefully.
    end
  end

  # Native Ruby client for the VELOCITY-WorkFlow engine.
  #
  # Uses FFI to call the native velocity_workflow_engine shared library.
  # Falls back to a mock mode if the library is not available.
  #
  # @example
  #   client = VelocitySdk::VelocityClient.new
  #   key = client.start_workflow("my-workflow", total_steps: 5)
  #   client.signal_workflow(key, "my-signal", "payload")
  #   status = client.get_status(key)
  #   client.close
  class VelocityClient
    # @return [String] Target address (for display purposes).
    attr_reader :target

    # Connect to a VELOCITY-WorkFlow server or load the native engine.
    #
    # @param target [String] gRPC server address (e.g. "localhost:7234").
    # @param jwt_token [String, nil] Optional JWT bearer token.
    # @param library_path [String, nil] Optional path to the native engine library.
    def initialize(target: 'localhost:7234', jwt_token: nil, library_path: nil)
      @target = target
      @jwt_token = jwt_token
      @interceptors = InterceptorChain.new
      @engine_handle = nil
      @ffi_available = false

      # Attempt to load the native engine via FFI.
      if EngineFfi.try_load(library_path)
        EngineFfi.setup_functions
        begin
          @engine_handle = EngineFfi.velocity_engine_create
          @ffi_available = !@engine_handle.null?
        rescue FFI::NullPointerError, NoMethodError
          @ffi_available = false
        end
      end
    end

    # Access the interceptor chain.
    # @return [InterceptorChain]
    def interceptors
      @interceptors
    end

    # Start a new workflow execution.
    #
    # @param workflow_type [String] Workflow type name.
    # @param namespace [String] Namespace to run in.
    # @param task_queue [String] Task queue for worker dispatch.
    # @param total_steps [Integer] Number of execution steps.
    # @param input [String] Optional input payload.
    # @return [Integer] Workflow key (0 on failure).
    def start_workflow(workflow_type, namespace: 'default', task_queue: 'default', total_steps: 1, input: '')
      type_id = Zlib.crc32(workflow_type)
      ns_id = Zlib.crc32(namespace)
      tq_hash = Zlib.crc32(task_queue)

      if @ffi_available
        key = EngineFfi.velocity_engine_start_workflow(
          @engine_handle,
          type_id, type_id, ns_id, tq_hash,
          total_steps,
          input.empty? ? nil : FFI::MemoryPointer.from_string(input),
          input.bytesize,
        )
        @interceptors.invoke_start(workflow_type, key)
        return key
      end

      raise ConnectionError.new(@target, 'No FFI or gRPC backend available')
    end

    # Get the current status of a workflow.
    #
    # @param workflow_key [Integer] Workflow key.
    # @return [Symbol] Status symbol (:running, :completed, :failed, etc.)
    def get_status(workflow_key)
      if @ffi_available
        code = EngineFfi.velocity_engine_get_status(@engine_handle, workflow_key)
        return WorkflowStatus.name_for(code).to_sym
      end

      raise ConnectionError.new(@target, 'No backend available')
    end

    # Signal a running workflow.
    #
    # @param workflow_key [Integer] Workflow key.
    # @param signal_name [String] Signal name.
    # @param payload [String] Signal payload.
    # @return [Boolean]
    def signal_workflow(workflow_key, signal_name, payload = '')
      if @ffi_available
        signal_id = Zlib.crc32(signal_name)
        ptr = payload.empty? ? nil : FFI::MemoryPointer.from_string(payload)
        EngineFfi.velocity_engine_signal_workflow(@engine_handle, workflow_key, signal_id, ptr, payload.bytesize)
        @interceptors.invoke_signal(workflow_key, signal_name)
        return true
      end

      raise ConnectionError.new(@target, 'No backend available')
    end

    # Cancel a running workflow.
    #
    # @param workflow_key [Integer] Workflow key.
    # @return [Boolean]
    def cancel_workflow(workflow_key)
      if @ffi_available
        EngineFfi.velocity_engine_cancel_workflow(@engine_handle, workflow_key)
        return true
      end

      raise ConnectionError.new(@target, 'No backend available')
    end

    # Close the client and release resources.
    def close
      if @ffi_available && @engine_handle && !@engine_handle.null?
        EngineFfi.velocity_engine_destroy(@engine_handle)
        @engine_handle = nil
        @ffi_available = false
      end
    end

    # Ensure resources are released on GC.
    def finalize
      close
    end
  end
end
