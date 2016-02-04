require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe WorkerContext do

        let(:worker_class) { Class.new {} }

        describe '#call' do
          let(:worker) { worker_class.new }
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:job_id) { SecureRandom.uuid }
          let(:parent_job_id) { SecureRandom.uuid }
          #noinspection RubyStringKeysInHashInspection
          let(:job) { {'queue' => queue_name, 'resource' => resource_name, 'jid' => job_id, 'pjid' => parent_job_id} }
          let(:original_message) { Sidekiq.load_json(subject.original_message) }

          it 'should yield' do
            expect { |block| subject.call(worker, job, nil, &block) }.to yield_control
          end

          context 'when the worker responds to #setup' do
            let(:worker_class) { Struct.new(:is_setup) { define_method(:setup) { self.is_setup = true } } }

            it 'should call #setup before yielding' do
              subject.call(worker, job, nil) do
                expect(worker.is_setup).to eq(true)
              end
            end
          end

          context 'when the worker responds to #teardown' do
            let(:worker_class) { Struct.new(:is_down) { define_method(:teardown) { self.is_down = true } } }

            it 'should call #teardown' do
              subject.call(worker, job, nil) {}
              expect(worker.is_down).to eq(true)
            end

            it 'should call #teardown after yielding' do
              subject.call(worker, job, nil) do
                expect(worker.is_down).not_to eq(true)
              end
            end

            context 'when the block raises an error' do
              it 'ensures that #teardown is still called' do
                subject.call(worker, job, nil) { raise 'It broke!' } rescue nil
                expect(worker.is_down).to eq(true)
              end
            end
          end

          context 'when the worker responsds to #context' do
            let(:block) do
              ->(_self, &block) do
                _self.has_context = true
                block.call
                _self.has_context = false
              end
            end
            let(:worker_class) do
              Struct.new(:callback, :has_context) do
                define_method(:context) do |&block|
                  callback.call(self, &block)
                end
              end
            end
            let(:worker) { worker_class.new(block) }

            it 'should yield within #context' do
              subject.call(worker, job, nil) do
                expect(worker.has_context).to eq(true)
              end
            end

            context 'when #context does not yield' do
              let(:block) { ->(_) {} }

              it 'should raise an error' do
                expect { subject.call(worker, job, nil) }.to raise_error("Expected worker '#{worker_class}' #context to yield, but it didn't!")
              end
            end
          end
        end

      end
    end
  end
end
