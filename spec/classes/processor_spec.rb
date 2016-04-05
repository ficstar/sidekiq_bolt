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
    let(:job) { {'class' => 'Sidekiq::MockProcessorWorker', 'args' => args} }
    let(:queue) { Faker::Lorem.word }
    let(:serialized_job) { Sidekiq.dump_json(job) }
    let(:work) { double(:work, queue_name: queue, message: serialized_job, acknowledge: nil) }
    let(:processor) { Processor.new(boss) }

    before do
      allow(Sidekiq.server_middleware).to receive(:invoke).with(a_kind_of(MockProcessorWorker), job, queue).and_yield
    end

    describe '#process' do
      it 'should call real_thread on the boss actor' do
        expect(async).to receive(:real_thread).with(processor.proxy_id, a_kind_of(Celluloid::Thread))
        processor.process(work)
      end

      it 'should execute the worker with the provided arguments' do
        expect_any_instance_of(MockProcessorWorker).to receive(:perform).with(*args)
        processor.process(work)
      end

      it 'should acknowledge the work' do
        expect(work).to receive(:acknowledge)
        processor.process(work)
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

      describe 'work dispatch' do
        describe 'performing the work asynchronously' do

          context 'when the work is not yet complete' do
            it 'should not acknowledge incomplete work' do
              allow(ThomasUtils::Future).to receive(:immediate)
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
