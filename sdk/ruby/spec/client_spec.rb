# frozen_string_literal: true

require_relative '../lib/velocity_sdk'

RSpec.describe VelocitySdk do
  describe VelocitySdk::MockClient do
    let(:client) { described_class.new }

    it 'starts a workflow and returns a positive key' do
      key = client.start_workflow('test-workflow')
      expect(key).to be > 0
    end

    it 'returns running status after start' do
      key = client.start_workflow('test-workflow', total_steps: 5)
      desc = client.describe_workflow(key)
      expect(desc[:status]).to eq(VelocitySdk::WorkflowStatus::RUNNING)
      expect(desc[:total_steps]).to eq(5)
    end

    it 'completes a workflow' do
      key = client.start_workflow('test-workflow')
      expect(client.complete_workflow(key, 'done')).to be true
      expect(client.get_status(key)).to eq(VelocitySdk::WorkflowStatus::COMPLETED)
    end

    it 'raises WorkflowNotFoundError for nonexistent workflow' do
      expect { client.describe_workflow(99_999) }.to raise_error(VelocitySdk::WorkflowNotFoundError)
    end

    it 'raises WorkflowAlreadyCompletedError on double complete' do
      key = client.start_workflow('test-workflow')
      client.complete_workflow(key)
      expect { client.complete_workflow(key) }.to raise_error(VelocitySdk::WorkflowAlreadyCompletedError)
    end

    it 'signals a workflow' do
      key = client.start_workflow('test-workflow')
      expect(client.signal_workflow(key, 'my-signal', 'payload')).to be true
      signals = client.get_signals(key)
      expect(signals.size).to eq(1)
      expect(signals.first[:signal_name]).to eq('my-signal')
    end

    it 'cancels a workflow' do
      key = client.start_workflow('test-workflow')
      expect(client.cancel_workflow(key)).to be true
      expect(client.get_status(key)).to eq(VelocitySdk::WorkflowStatus::CANCELED)
    end

    it 'lists workflow keys' do
      key1 = client.start_workflow('wf-1')
      key2 = client.start_workflow('wf-2')
      keys = client.list_workflows
      expect(keys).to include(key1, key2)
    end
  end

  describe VelocitySdk::TestWorkflowEnvironment do
    let(:env) { described_class.new }

    it 'asserts workflow completed' do
      key = env.start_workflow('test-workflow')
      env.complete_workflow(key, 'ok')
      expect { env.assert_workflow_completed(key) }.not_to raise_error
    end

    it 'asserts signal received' do
      key = env.start_workflow('test-workflow')
      env.signal_workflow(key, 'approval', 'yes')
      expect { env.assert_signal_received(key, 'approval') }.not_to raise_error
      expect { env.assert_signal_received(key, 'missing') }.to raise_error(RuntimeError, /Expected signal/)
    end

    it 'resets the environment' do
      env.start_workflow('test-workflow')
      env.reset
      expect(env.client.list_workflows).to be_empty
    end

    it 'supports time skip' do
      before_time = env.current_time_secs
      env.time_skip(3600)
      expect(env.current_time_secs).to be >= before_time + 3600
    end
  end

  describe VelocitySdk::InterceptorChain do
    it 'invokes interceptors in order' do
      chain = described_class.new
      metrics = VelocitySdk::MetricsInterceptor.new
      chain.add(metrics)
      chain.add(VelocitySdk::LoggingInterceptor.new)

      chain.invoke_start('test-type', 1)
      chain.invoke_complete(1, 'result')

      snapshot = metrics.snapshot
      expect(snapshot[:workflow_starts]).to eq(1)
      expect(snapshot[:workflow_completions]).to eq(1)
    end
  end

  describe VelocitySdk::WorkflowStatus do
    it 'maps status codes to names' do
      expect(described_class.name_for(1)).to eq('running')
      expect(described_class.name_for(2)).to eq('completed')
      expect(described_class.name_for(99)).to eq('unknown')
    end
  end
end
