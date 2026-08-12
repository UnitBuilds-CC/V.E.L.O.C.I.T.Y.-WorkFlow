# frozen_string_literal: true

require_relative '../lib/velocity_sdk'
require_relative '../lib/velocity_sdk/workflow_execution'
require_relative '../lib/velocity_sdk/workflow_options'
require_relative '../lib/velocity_sdk/activity_options'
require_relative '../lib/velocity_sdk/activity_stub'
require_relative '../lib/velocity_sdk/connection'

RSpec.describe 'VelocitySdk Integration' do
  let(:client) { VelocitySdk::MockClient.new }

  describe 'workflow lifecycle with options' do
    it 'starts, describes, and completes a workflow' do
      options = VelocitySdk::WorkflowOptions.new
        .namespace('integration-test')
        .task_queue('high-priority')
        .total_steps(3)
        .execution_timeout_ms(5_000)

      expect(options.namespace).to eq('integration-test')
      expect(options.task_queue).to eq('high-priority')
      expect(options.total_steps).to eq(3)

      key = client.start_workflow('lifecycle-wf', total_steps: 3)
      expect(key).to be > 0

      desc = client.describe_workflow(key)
      expect(desc[:status]).to eq(VelocitySdk::WorkflowStatus::RUNNING)
      expect(desc[:total_steps]).to eq(3)

      expect(client.complete_workflow(key, 'done')).to be true
      desc = client.describe_workflow(key)
      expect(desc[:status]).to eq(VelocitySdk::WorkflowStatus::COMPLETED)
    end
  end

  describe VelocitySdk::WorkflowExecution do
    it 'tracks execution state' do
      exec = described_class.new(
        key: 42,
        workflow_type: 'test-type',
        namespace: 'prod',
        status: 'running',
      )

      expect(exec.key).to eq(42)
      expect(exec.workflow_type).to eq('test-type')
      expect(exec.namespace).to eq('prod')
      expect(exec).to be_running
      expect(exec).not_to be_terminal

      exec.status = 'completed'
      expect(exec).not_to be_running
      expect(exec).to be_terminal
    end

    it 'serialises to a hash' do
      exec = described_class.new(key: 1, workflow_type: 'wf', status: 'running')
      h = exec.to_h
      expect(h[:key]).to eq(1)
      expect(h[:workflow_type]).to eq('wf')
      expect(h[:status]).to eq('running')
    end
  end

  describe VelocitySdk::WorkflowOptions do
    it 'builds options with fluent interface' do
      opts = described_class.new
        .namespace('ns1')
        .task_queue('tq1')
        .workflow_id('wf-123')
        .memo('test memo')
        .search_attributes(env: 'prod')

      h = opts.to_h
      expect(h[:namespace]).to eq('ns1')
      expect(h[:task_queue]).to eq('tq1')
      expect(h[:workflow_id]).to eq('wf-123')
      expect(h[:memo]).to eq('test memo')
      expect(h[:search_attributes]).to eq(env: 'prod')
    end

    it 'enforces minimum total_steps of 1' do
      opts = described_class.new.total_steps(0)
      expect(opts.total_steps).to eq(1)
    end
  end

  describe VelocitySdk::ActivityOptions do
    it 'has sensible defaults' do
      opts = described_class.defaults
      expect(opts.start_to_close_timeout_ms).to eq(10_000)
      expect(opts.schedule_to_close_timeout_ms).to eq(60_000)
      expect(opts.heartbeat_timeout_ms).to eq(0)
      expect(opts.retry_max_attempts).to eq(1)
    end
  end

  describe VelocitySdk::ActivityStub do
    it 'executes and tracks count' do
      stub = described_class.new(client, 'ProcessPayment',
        VelocitySdk::ActivityOptions.new.start_to_close_timeout_ms(5_000).retry_max_attempts(3))

      expect(stub.activity_type).to eq('ProcessPayment')
      expect(stub.execution_count).to eq(0)

      result = stub.execute('{"amount": 100}')
      expect(result).not_to be_empty
      expect(stub.execution_count).to eq(1)

      decoded = JSON.parse(result)
      expect(decoded['activity_type']).to eq('ProcessPayment')
      expect(decoded['attempt']).to eq(1)
    end

    it 'generates unique async keys' do
      stub = described_class.new(client, 'SendEmail')
      key1 = stub.execute_async('to=user@example.com')
      key2 = stub.execute_async('to=admin@example.com')
      expect(key1).not_to eq(key2)
      expect(stub.execution_count).to eq(2)
    end
  end

  describe 'signal and cancel workflow' do
    it 'signals and cancels correctly' do
      key = client.start_workflow('signal-test-wf')

      expect(client.signal_workflow(key, 'approve', 'yes')).to be true
      signals = client.get_signals(key)
      expect(signals.size).to eq(1)
      expect(signals.first[:signal_name]).to eq('approve')

      expect(client.cancel_workflow(key)).to be true
      desc = client.describe_workflow(key)
      expect(desc[:status]).to eq(VelocitySdk::WorkflowStatus::CANCELED)
    end
  end

  describe 'multiple workflows isolation' do
    it 'keeps workflows independent' do
      key1 = client.start_workflow('wf-a')
      key2 = client.start_workflow('wf-b')

      expect(key1).not_to eq(key2)

      client.complete_workflow(key1, 'result-a')
      expect(client.describe_workflow(key1)[:status]).to eq(VelocitySdk::WorkflowStatus::COMPLETED)
      expect(client.describe_workflow(key2)[:status]).to eq(VelocitySdk::WorkflowStatus::RUNNING)
    end
  end
end
