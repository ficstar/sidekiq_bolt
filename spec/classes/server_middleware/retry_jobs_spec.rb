require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe RetryJobs do

        let(:worker_class) do
          Class.new do
            include Worker
          end
        end
        let(:worker) { worker_class.new }

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:retry_job) { true }
          let(:job) { {'queue' => queue_name, 'resource' => resource_name, 'retry' => retry_job} }

          it 'should yield' do
            expect { |block| subject.call(worker, job, nil, &block) }.to yield_control
          end

          context 'when the block raises an error' do
            let(:error) { StandardError.new(Faker::Lorem.sentence).tap { |err| err.set_backtrace(Faker::Lorem.paragraph) } }
            let(:resource) { Resource.new(resource_name) }
            let(:error_work) { resource.allocate(1) }
            let(:error_queue) { error_work[0] }
            let(:error_job) { Sidekiq.load_json(error_work[1]) }

            it 'should count this work as retrying' do
              subject.call(worker, job, nil) { raise error }
              expect(resource.retrying).to eq(1)
            end

            it 'should add this work back to the resource' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job).to include(job)
            end

            it 'should add it for the original queue' do
              subject.call(worker, job, nil) { raise error }
              expect(error_queue).to eq(queue_name)
            end

            it 'should include the error information in the retrying job' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job['error']).to eq(error)
            end

            it 'should increment the number of times this error has been hit' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job["retry_count:#{error}"]).to eq(1)
            end

            context 'when this work cannot retry' do
              let(:retry_job) { false }

              it 'should raise the error' do
                expect { subject.call(worker, job, nil) { raise error } }.to raise_error(error)
              end
            end

            context 'when the worker specifies a sidekiq_should_retry_block' do
              let(:worker_class) do
                Class.new do
                  include Worker
                  should_retry? { |job, error| !error.is_a?(StandardError) && !job['borked!'] }
                end
              end

              it 'should use that block to determine whether or not to retry' do
                expect { subject.call(worker, job, nil) { raise error } }.to raise_error(error)
              end

              context 'with a different error' do
                let(:error) { Interrupt.new }

                it 'should use that block to determine whether or not to retry' do
                  expect { subject.call(worker, job, nil) { raise error } }.not_to raise_error
                end
              end

              context 'when the job contains something that the worker does not like' do
                before { job['borked!'] = true }

                it 'should not retry' do
                  expect { subject.call(worker, job, nil) { raise error } }.to raise_error(error)
                end
              end
            end
          end

        end

      end
    end
  end
end
