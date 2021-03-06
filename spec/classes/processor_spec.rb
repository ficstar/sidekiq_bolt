require 'rspec'

module Sidekiq
  describe Processor do
    class MockProcessorWorker
      include Bolt::Worker

      def perform(*)

      end
    end

    let(:async) { double(:actor_async, real_thread: nil, processor_done: nil) }
    let(:boss) { double(:actor, async: async) }
    let(:args) { Faker::Lorem.words }
    let(:job) { Bolt::Message['class' => 'Sidekiq::MockProcessorWorker', 'args' => args] }
    let(:queue) { Faker::Lorem.word }
    let(:serialized_job) { Sidekiq.dump_json(job) }
    let(:allocation) { rand(1..100) }
    let(:work) { work_klass.new(queue, nil, allocation, serialized_job) }
    let(:processor) { Processor.new(boss) }

    before do
      allow(Sidekiq.server_middleware).to receive(:invoke).with(a_kind_of(MockProcessorWorker), job, queue).and_yield
      allow_any_instance_of(Processor).to receive(:handle_exception)
    end

    describe '#process' do
      it 'should call real_thread on the boss actor' do
        expect(async).to receive(:real_thread).with(processor.proxy_id, a_kind_of(Celluloid::Thread))
        processor.process(work)
      end

      it 'should set the resource allocation on this worker' do
        expect_any_instance_of(MockProcessorWorker).to receive(:resource_allocation=).with(allocation)
        processor.process(work)
      end

      it 'should execute the worker with the provided arguments' do
        expect_any_instance_of(MockProcessorWorker).to receive(:perform).with(*args)
        processor.process(work)
      end

      it 'should execute the worker successively using a Future with an immediate executor' do
        expect(ThomasUtils::Future).to receive(:successive).with(executor: :immediate) do |&block|
          expect_any_instance_of(MockProcessorWorker).to receive(:perform)
          block.call
        end
        processor.process(work)
      end

      context 'with a custom processor type set' do
        let(:processor_type) { Faker::Lorem.word }
        let(:expected_executor) { :"sidekiq_bolt_#{work.processor_type}" }

        before { work.processor_type = processor_type }

        it 'should execute the worker successively using a Future with the specified executor' do
          expect(ThomasUtils::Future).to receive(:successive).with(executor: expected_executor) do |&block|
            expect_any_instance_of(MockProcessorWorker).to receive(:perform)
            block.call
          end
          processor.process(work)
        end
      end

      it 'should acknowledge the work' do
        expect(work).to receive(:acknowledge)
        processor.process(work)
      end

      it 'should return an Observation' do
        expect(processor.process(work)).to be_a_kind_of(ThomasUtils::Observation)
      end

      context 'when the server middleware does not yield' do
        it 'should not execute the worker' do
          allow(Sidekiq.server_middleware).to receive(:invoke).with(a_kind_of(MockProcessorWorker), job, queue)
          expect_any_instance_of(MockProcessorWorker).not_to receive(:perform)
          processor.process(work)
        end

        it 'should NOT acknowledge the work' do
          allow(Sidekiq.server_middleware).to receive(:invoke).with(a_kind_of(MockProcessorWorker), job, queue)
          expect(work).not_to receive(:acknowledge)
          processor.process(work)
        end
      end

      context 'when the block raises an error' do
        let(:error) { Interrupt.new }

        before { allow_any_instance_of(MockProcessorWorker).to receive(:perform).and_raise(error) }

        it 'should still acknowledge the work' do
          expect(work).to receive(:acknowledge)
          processor.process(work)
        end

        it 'should execute the error handler for Sidekiq' do
          expect_any_instance_of(Processor).to receive(:handle_exception).with(error, job)
          processor.process(work)
        end

        it 'should raise the error on resolution' do
          expect { processor.process(work).get }.to raise_error(error)
        end
      end

      describe 'work dispatch' do
        describe 'performing the work asynchronously' do
          let(:mock_future) { ThomasUtils::Future.none }

          context 'when the work is not yet complete' do
            it 'should not acknowledge incomplete work' do
              allow(ThomasUtils::Future).to receive(:immediate).and_return(mock_future)
              expect(work).not_to receive(:acknowledge)
              processor.process(work)
            end
          end

        end
      end

      it 'should indicate when it is done' do
        expect(async).to receive(:processor_done).with(processor.current_actor)
        processor.process(work)
      end
    end

  end
end
