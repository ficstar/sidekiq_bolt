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

            context 'when the worker specified a sidekiq_retry_in_block' do
              let(:retry_count) { rand(1..10) }
              let(:worker_class) do
                Class.new do
                  include Worker
                  sidekiq_retry_in do |count, error|
                    count * (error.is_a?(StandardError) ? 2 : 7)
                  end
                end
              end
              let(:serialized_msg) { global_redis.zrange('bolt:retry', 0, -1).first }
              let(:msg) { JSON.load(serialized_msg) if serialized_msg }
              let(:now) { Time.at(7777777777) }
              let(:expected_retry_score) { Time.now.to_f + 2 * retry_count }
              let(:retry_msg_job) { Sidekiq.load_json(msg['work']) if msg['work'] }

              around { |example| Timecop.freeze(now) { example.run } }

              before do
                job["retry_count:#{error}"] = retry_count
                subject.call(worker, job, nil) { raise error }
              end

              it 'should add this job to the retry set for the queue for this job' do
                expect(msg['queue']).to eq(queue_name)
              end

              it 'should add this job to the retry set for the resource for this job' do
                expect(msg['resource']).to eq(resource_name)
              end

              it 'should save the job' do
                expect(retry_msg_job).to include(job)
              end

              it 'should only retry the job once' do
                expect(resource.retrying).to eq(1)
              end

              it 'should retry the job in the time specified by the worker' do
                expect(global_redis.zscore('bolt:retry', serialized_msg)).to be_within(2).of(expected_retry_score)
              end

              context 'with a different error' do
                let(:error) { Interrupt.new }
                let(:expected_retry_score) { Time.now.to_f + 7 * retry_count }

                it 'should retry the job in the time specified by the worker' do
                  expect(global_redis.zscore('bolt:retry', serialized_msg)).to be_within(2).of(expected_retry_score)
                end
              end
            end

            context 'when the worker specifies a sidekiq_should_retry_block' do
              let(:worker_class) do
                Class.new do
                  include Worker
                  should_retry? do |job, error, hit_count|
                    !error.is_a?(StandardError) && !job['borked!'] && hit_count.to_i < 10
                  end
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

              context 'when the error count is used to determine whether or not to retry' do
                let(:error) { Interrupt.new }

                before { job["retry_count:#{error}"] = 50 }

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
