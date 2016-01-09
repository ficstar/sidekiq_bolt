require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe RetryJobs do

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:retry_job) { true }
          let(:job) { {'queue' => queue_name, 'resource' => resource_name, 'retry' => retry_job} }

          it 'should yield' do
            expect { |block| subject.call(nil, job, nil, &block) }.to yield_control
          end

          context 'when the block raises an error' do
            let(:error) { StandardError.new(Faker::Lorem.sentence).tap { |err| err.set_backtrace(Faker::Lorem.paragraph) } }
            let(:resource) { Resource.new(resource_name) }
            let(:error_work) { resource.allocate(1) }
            let(:error_queue) { error_work[0] }
            let(:error_job) { Sidekiq.load_json(error_work[1]) }

            it 'should count this work as retrying' do
              subject.call(nil, job, nil) { raise error }
              expect(resource.retrying).to eq(1)
            end

            it 'should add this work back to the resource' do
              subject.call(nil, job, nil) { raise error }
              expect(error_job).to include(job)
            end

            it 'should add it for the original queue' do
              subject.call(nil, job, nil) { raise error }
              expect(error_queue).to eq(queue_name)
            end

            it 'should include the error information in the retrying job' do
              subject.call(nil, job, nil) { raise error }
              expect(error_job['error']).to eq(error)
            end

            context 'when this work cannot retry' do
              let(:retry_job) { false }

              it 'should raise the error' do
                expect { subject.call(nil, job, nil) { raise error } }.to raise_error(error)
              end
            end
          end

        end

      end
    end
  end
end
