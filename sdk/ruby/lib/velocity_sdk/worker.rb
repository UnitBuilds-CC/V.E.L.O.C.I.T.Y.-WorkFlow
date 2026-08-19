# frozen_string_literal: true

require_relative 'auto_apply'

module VelocitySDK
  # Context available inside workflow functions.
  #
  # Provides deterministic operations for scheduling activities, timers,
  # signals, queries, updates, and child workflows.
  class WorkflowContext
    attr_reader :workflow_key, :workflow_id, :run_id, :workflow_type, :task_queue
    attr_accessor :current_step

    def initialize(workflow_key:, workflow_id:, run_id:, workflow_type:, task_queue:, client: nil)
      @workflow_key = workflow_key
      @workflow_id = workflow_id
      @run_id = run_id
      @workflow_type = workflow_type
      @task_queue = task_queue
      @client = client
      @current_step = 0
      @signal_handlers = {}
      @query_handlers = {}
      @update_handlers = {}
      @pending_signals = {}
    end

    # Schedule an activity for execution.
    def execute_activity(activity_name, *args, **kwargs)
      @current_step += 1

      # In a full implementation, this would send a command to the server.
      # For embedded/local mode, call the registered activity directly.
      activities = AutoApply::Registry.registered_activities
      handler = activities[activity_name]

      raise "No activity registered for '#{activity_name}'" unless handler

      handler.call(*args, **kwargs)
    end

    # Deterministic timer.
    def sleep(duration_ms)
      @current_step += 1
      Kernel.sleep(duration_ms / 1000.0)
    end

    # Register a signal handler.
    def on_signal(signal_name, &handler)
      @signal_handlers[signal_name] = handler
    end

    # Register a query handler.
    def on_query(query_name, &handler)
      @query_handlers[query_name] = handler
    end

    # Register an update handler.
    def on_update(update_name, &handler)
      @update_handlers[update_name] = handler
    end

    # Block until a signal is received.
    def wait_for_signal(signal_name)
      if @pending_signals[signal_name]&.any?
        return @pending_signals[signal_name].shift
      end

      # In production, this suspends the workflow until the signal arrives.
      raise "Waiting for signal '#{signal_name}' — not yet buffered"
    end

    # Start a child workflow.
    def start_child_workflow(workflow_type, *args, **kwargs)
      @current_step += 1
      raise "Child workflows require server-side support"
    end

    # Check if the workflow is canceled.
    def canceled?
      false
    end
  end

  # Worker process model for the VELOCITY-WorkFlow Ruby SDK.
  #
  # The Worker polls the server for workflow and activity tasks, executes them
  # using auto-registered (or manually registered) implementations, and reports
  # results back. Supports the auto-apply DSL system for zero-config workflow
  # discovery.
  #
  # @example
  #   # Auto-apply mode — DSL registers workflows automatically
  #   class OrderWorkflow
  #     include VelocitySDK::Workflow
  #
  #     def run(ctx, order_id)
  #       ctx.execute_activity('process_payment', order_id)
  #     end
  #   end
  #
  #   VelocitySDK.activity(:process_payment) do |order_id|
  #     { status: 'charged', order_id: order_id }
  #   end
  #
  #   # Worker auto-discovers all workflows and activities
  #   worker = VelocitySDK::Worker.new(task_queue: 'orders')
  #   worker.run
  #
  # @example
  #   # Manual registration mode
  #   worker = VelocitySDK::Worker.new(task_queue: 'orders')
  #   worker.register_workflow('OrderWorkflow', OrderWorkflow)
  #   worker.register_activity('process_payment', ->(order_id) { { status: 'charged' } })
  #   worker.run
  class Worker
    attr_reader :task_queue, :stats

    def initialize(
      task_queue:,
      server_address: 'localhost:7234',
      namespace: 'default',
      max_concurrent_workflow_tasks: 10,
      max_concurrent_activity_tasks: 100,
      poll_timeout_ms: 10_000,
      heartbeat_interval_ms: 30_000,
      build_id: '1.0',
      client: nil
    )
      @task_queue = task_queue
      @server_address = server_address
      @namespace = namespace
      @max_concurrent_workflow_tasks = max_concurrent_workflow_tasks
      @max_concurrent_activity_tasks = max_concurrent_activity_tasks
      @poll_timeout_ms = poll_timeout_ms
      @heartbeat_interval_ms = heartbeat_interval_ms
      @build_id = build_id
      @client = client || VelocityClient.new(target: server_address)
      @identity = "ruby-worker-#{build_id}@#{Socket.gethostname rescue 'unknown'}"
      @running = false
      @workflows = {}
      @activities = {}
      @stats = {
        workflows_started: 0,
        workflows_completed: 0,
        workflows_failed: 0,
        activities_scheduled: 0,
        activities_completed: 0,
        activities_failed: 0,
        tasks_polled: 0,
        heartbeats_sent: 0,
        start_time: Time.now,
      }
    end

    # Manually register a workflow class.
    def register_workflow(workflow_type, klass)
      @workflows[workflow_type] = klass
    end

    # Manually register an activity handler.
    def register_activity(activity_name, handler)
      @activities[activity_name] = handler
    end

    # Auto-discover workflows and activities from the registry.
    def auto_discover
      # Merge auto-apply registry with manual registrations
      auto_workflows = AutoApply::Registry.registered_workflows
      auto_activities = AutoApply::Registry.registered_activities

      auto_workflows.each do |workflow_type, klass|
        @workflows[workflow_type] = klass unless @workflows.key?(workflow_type)
      end

      auto_activities.each do |activity_name, handler|
        @activities[activity_name] = handler unless @activities.key?(activity_name)
      end
    end

    # Start the worker and block until shutdown.
    def run
      auto_discover
      @running = true

      # Install signal handlers for graceful shutdown
      trap('INT') { shutdown }
      trap('TERM') { shutdown }

      while @running
        @stats[:tasks_polled] += 1

        begin
          # Poll for workflow tasks via gRPC long-poll
          wf_task = @client.poll_workflow_task_queue(
            @task_queue,
            namespace: @namespace,
            identity: @identity,
            build_id: @build_id,
            timeout_ms: @poll_timeout_ms,
          )

          if wf_task
            execute_workflow_task(wf_task)
            next
          end

          # Poll for activity tasks
          act_task = @client.poll_activity_task_queue(
            @task_queue,
            namespace: @namespace,
            identity: @identity,
            build_id: @build_id,
            timeout_ms: @poll_timeout_ms,
          )

          if act_task
            execute_activity_task(act_task)
            next
          end

          # No tasks available — brief backoff
          sleep(0.1)
        rescue => e
          warn "[velocity-worker] Poll error: #{e.message}"
          sleep(1)
        end
      end

      @client&.close
    end

    # Request graceful shutdown.
    def shutdown
      @running = false
    end

    # Check if the worker is running.
    def running?
      @running
    end

    # Get a snapshot of worker statistics.
    def stats_snapshot
      @stats.merge(
        uptime_ms: ((Time.now - @stats[:start_time]) * 1000).to_i,
        registered_workflows: @workflows.size,
        registered_activities: @activities.size,
      )
    end

    private

    # Execute a workflow task.
    def execute_workflow_task(task)
      workflow_type = task[:workflow_type] || ''
      task_token = task[:task_token] || 0
      workflow_key = task[:workflow_key] || 0
      workflow_id = task[:workflow_id] || "wf-#{workflow_key}"
      input = task[:input] || '{}'

      klass = @workflows[workflow_type]
      unless klass
        warn "[velocity-worker] No workflow registered for type: #{workflow_type}"
        @client.respond_workflow_task_completed(task_token,
          commands: [{ fail_workflow: { reason: "No workflow registered for type: #{workflow_type}" } }],
          identity: @identity, namespace: @namespace)
        return
      end

      @stats[:workflows_started] += 1

      begin
        instance = klass.new
        context = WorkflowContext.new(
          workflow_key: workflow_key,
          workflow_id: workflow_id,
          run_id: "run-#{(Time.now.to_f * 1000).to_i}",
          workflow_type: workflow_type,
          task_queue: @task_queue,
          client: @client,
        )

        args = JSON.parse(input, symbolize_names: true)
        args = args.is_a?(Hash) ? args : {}

        result = if instance.respond_to?(:run)
                   instance.run(context, **args)
                 elsif instance.respond_to?(:call)
                   instance.call(context, **args)
                 else
                   raise "Workflow '#{workflow_type}' has no 'run' method"
                 end

        result_bytes = JSON.generate(result)

        # Report completion to server with complete_workflow command
        @client.respond_workflow_task_completed(task_token,
          commands: [{ complete_workflow: { result: [result_bytes].pack('m0') } }],
          identity: @identity, namespace: @namespace)

        @stats[:workflows_completed] += 1

      rescue => e
        @stats[:workflows_failed] += 1
        warn "[velocity-worker] Workflow '#{workflow_type}' failed: #{e.message}"

        # Report failure to server with fail_workflow command
        @client.respond_workflow_task_completed(task_token,
          commands: [{ fail_workflow: { reason: e.message } }],
          identity: @identity, namespace: @namespace)
      end
    end

    # Execute an activity task.
    def execute_activity_task(task)
      activity_type = task[:activity_type] || ''
      task_token = task[:task_token] || 0
      input = task[:input] || '{}'

      handler = @activities[activity_type]
      unless handler
        warn "[velocity-worker] No activity registered for type: #{activity_type}"
        @client.respond_activity_task_failed(task_token,
          failure: "No activity registered for type: #{activity_type}",
          identity: @identity, namespace: @namespace)
        return
      end

      @stats[:activities_scheduled] += 1

      begin
        args = JSON.parse(input, symbolize_names: true)
        args = args.is_a?(Array) ? args : [args]

        result = handler.call(*args)
        result_bytes = JSON.generate(result)

        # Report activity completion to server
        @client.respond_activity_task_completed(task_token,
          result: result_bytes, identity: @identity, namespace: @namespace)

        @stats[:activities_completed] += 1

      rescue => e
        @stats[:activities_failed] += 1
        warn "[velocity-worker] Activity '#{activity_type}' failed: #{e.message}"

        # Report activity failure to server
        @client.respond_activity_task_failed(task_token,
          failure: e.message, identity: @identity, namespace: @namespace)
      end
    end
  end
end
