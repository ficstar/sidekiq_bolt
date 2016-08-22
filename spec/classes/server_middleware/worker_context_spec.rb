require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe WorkerContext do

        let(:worker_class) { Struct.new(:resource) }

        describe '#call' do
          let(:worker) { worker_class.new(resource) }
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:resource) { Resource.new(resource_name) }
          let(:job_id) { SecureRandom.uuid }
          let(:parent_job_id) { SecureRandom.uuid }
          #noinspection RubyStringKeysInHashInspection
          let(:original_job) { Message['queue' => queue_name, 'resource' => resource_name, 'jid' => job_id, 'pjid' => parent_job_id] }
          let(:job) { original_job.dup }
          let(:original_message) { Sidekiq.dump_json(original_job) }

          it_behaves_like 'a server middleware'

          context 'when the worker responds to #setup' do
            let(:block_result) { true }
            let(:block) do
              result = block_result
              ->(_self) { _self.is_setup = true; result }
            end
            let(:worker_class) do
              Struct.new(:resource, :callback, :is_setup) do
                define_method(:setup) { callback.call(self) }
              end
            end
            let(:worker) { worker_class.new(resource, block) }
            let(:result_allocation) { work_klass.from_allocation(resource_name, resource.allocate(1)) }

            it 'should call #setup before yielding' do
              subject.call(worker, job, nil) do
                expect(worker.is_setup).to eq(true)
              end
            end

            it 'should not re-submit the work' do
              subject.call(worker, job, nil) {}
              expect(result_allocation).to be_nil
            end

            it 'should leave the job id alone' do
              subject.call(worker, job, nil) {}
              expect(job['jid']).to eq(job_id)
            end

            context 'when #setup returns false' do
              let(:block_result) { false }
              let(:result_queue) { result_allocation.queue }
              let(:result_work) { result_allocation.work }

              it 'should not yield' do
                expect { |block| subject.call(worker, job, nil, &block) }.not_to yield_control
              end

              it 'should re-submit the work' do
                subject.call(worker, job, nil) {}
                expect(result_work).to eq(original_message)
              end

              it 'should re-submit the work to the right queue' do
                subject.call(worker, job, nil) {}
                expect(result_queue).to eq(queue_name)
              end

              it 'should remove the job id so JobSuccession does not kick in' do
                subject.call(worker, job, nil) {}
                expect(job['jid']).to be_nil
              end
            end
          end

          context 'when the worker responds to #teardown' do
            let(:worker_class) { Struct.new(:resource, :is_down) { define_method(:teardown) { self.is_down = true } } }

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
              Struct.new(:resource, :callback, :has_context) do
                define_method(:context) do |&block|
                  callback.call(self, &block)
                end
              end
            end
            let(:worker) { worker_class.new(resource, block) }

            it 'should yield within #context' do
              subject.call(worker, job, nil) do
                expect(worker.has_context).to eq(true)
              end
            end

            context 'when #context does not yield' do
              let(:block) { ->(_) {} }

              it 'should raise an error' do
                expect { subject.call(worker, job, nil).get }.to raise_error("Expected worker '#{worker_class}' #context to yield, but it didn't!")
              end
            end
          end
        end

      end
    end
  end
end
