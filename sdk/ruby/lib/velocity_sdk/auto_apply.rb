# frozen_string_literal: true

module VelocitySDK
  # Auto-apply workflow and activity registration for the VELOCITY Ruby SDK.
  #
  # Provides a DSL for defining workflows and activities that are automatically
  # registered in a global registry. The Worker discovers all registered handlers
  # at startup — no manual wiring needed.
  #
  # @example
  #   class OrderWorkflow
  #     include VelocitySDK::Workflow
  #
  #     def run(ctx, order_id)
  #       ctx.execute_activity('process_payment', order_id)
  #     end
  #   end
  #
  #   class PaymentActivities
  #     include VelocitySDK::Activity
  #
  #     activity def process_payment(order_id)
  #       { status: 'charged', order_id: order_id }
  #     end
  #   end
  module AutoApply
    class Registry
      @workflows = {}
      @activities = {}
      @mutex = Mutex.new

      class << self
        # Register a workflow class.
        def register_workflow(workflow_type, klass)
          @mutex.synchronize { @workflows[workflow_type] = klass }
        end

        # Register an activity method.
        def register_activity(activity_name, handler)
          @mutex.synchronize { @activities[activity_name] = handler }
        end

        # Get all registered workflow types and their classes.
        def registered_workflows
          @mutex.synchronize { @workflows.dup }
        end

        # Get all registered activity names and their handlers.
        def registered_activities
          @mutex.synchronize { @activities.dup }
        end

        # Clear both registries (useful for testing).
        def clear
          @mutex.synchronize do
            @workflows.clear
            @activities.clear
          end
        end

        # Count of registered workflows.
        def workflow_count
          @mutex.synchronize { @workflows.size }
        end

        # Count of registered activities.
        def activity_count
          @mutex.synchronize { @activities.size }
        end
      end
    end
  end

  # Mixin for workflow classes.
  #
  # Include this module in a class to make it a durable workflow.
  # The class is automatically registered in the workflow registry.
  #
  # @example
  #   class OrderWorkflow
  #     include VelocitySDK::Workflow
  #
  #     def run(ctx, order_id)
  #       ctx.execute_activity('process_payment', order_id)
  #     end
  #   end
  module Workflow
    def self.included(base)
      base.extend(ClassMethods)
      # Register the workflow class automatically
      workflow_type = base.name || base.to_s
      AutoApply::Registry.register_workflow(workflow_type, base)
    end

    module ClassMethods
      # Set a custom workflow type name.
      def workflow_type(name)
        @custom_workflow_type = name
        # Re-register with the custom name
        AutoApply::Registry.register_workflow(name, self)
      end

      # Get the workflow type name.
      def workflow_type_name
        @custom_workflow_type || name || to_s
      end
    end

    # Instance method to check if this is a workflow.
    def velocity_workflow?
      true
    end
  end

  # Mixin for activity classes/modules.
  #
  # Include this module and use the `activity` class method to mark
  # methods as durable activities. They are automatically registered.
  #
  # @example
  #   class PaymentActivities
  #     include VelocitySDK::Activity
  #
  #     activity def process_payment(order_id)
  #       { status: 'charged', order_id: order_id }
  #     end
  #   end
  module Activity
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      # Mark a method as a durable activity.
      def activity(method_name)
        activity_name = method_name.to_s
        # Register the activity method
        AutoApply::Registry.register_activity(activity_name, method(method_name))
      end
    end
  end

  # Decorator-style DSL for standalone functions.
  #
  # @example
  #   VelocitySDK.activity(:process_payment) do |order_id|
  #     { status: 'charged', order_id: order_id }
  #   end
  def self.activity(name, &block)
    AutoApply::Registry.register_activity(name.to_s, block)
  end

  # Register a workflow class directly.
  #
  # @example
  #   VelocitySDK.workflow(OrderWorkflow)
  def self.workflow(klass)
    workflow_type = klass.name || klass.to_s
    AutoApply::Registry.register_workflow(workflow_type, klass)
  end
end
