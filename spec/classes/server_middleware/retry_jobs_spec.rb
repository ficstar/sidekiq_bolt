require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe RetryJobs do

        let(:worker_class) do
          Struct.new(:resource) do
            include Worker
          end
        end
        let(:worker) { worker_class.new(resource) }
        let(:resource_name) { Faker::Lorem.word }
        let(:resource) { Resource.new(resource_name) }

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:retry_job) { true }
          let(:job_id) { SecureRandom.uuid }
          let(:total_retries) { nil }
          let(:original_job_attributes) do
            {
                'queue' => queue_name,
                'resource' => resource_name,
                'retry' => retry_job,
                'jid' => job_id
            }
          end
          let(:original_job) do
            Message[original_job_attributes]
          end
          let(:job) { original_job.dup }

          before do
            if total_retries
              original_job['retry_count'] = {}
              original_job['retry_count']['total'] = total_retries
            end
          end

          it_behaves_like 'a server middleware'

          context 'when the block raises an error' do
            let(:error) { StandardError.new(Faker::Lorem.sentence).tap { |err| err.set_backtrace(Faker::Lorem.paragraph) } }
            let(:resource) { Resource.new(resource_name) }
            let(:error_work) { work_klass.from_allocation(resource_name, resource.allocate(1)) }
            let(:error_queue) { error_work.queue }
            let(:error_job) { Sidekiq.load_json(error_work.work) }

            it 'should count this work as retrying' do
              subject.call(worker, job, nil) { raise error }
              expect(resource.retrying).to eq(1)
            end

            context 'when a retry handler is provided for this error type' do
              let(:error_handler_class) { error.class }
              let(:error_handler_result) { Faker::Lorem.sentence }
              let(:error_handler_block) { ->(_, _, _) { error_handler_result } }
              let(:sidekiq_options) do
                {
                    concurrency: 0,
                    error_handlers: {error_handler_class => error_handler_block}
                }
              end

              it 'should NOT count this work as retrying' do
                subject.call(worker, job, nil) { raise error }
                expect(resource.retrying).to eq(0)
              end

              it 'should return the value of the handler block' do
                result = subject.call(worker, job, nil) { raise error }.get
                expect(result).to eq(error_handler_result)
              end

              describe 'calling the error handler' do
                let(:error_handler_block) { double(:callback) }

                it 'should call the error handler with the worker, job and error' do
                  expect(error_handler_block).to receive(:call).with(worker, job, error)
                  subject.call(worker, job, nil) { raise error }.get
                end
              end

              context 'when the error handler itself raises an error' do
                let(:error_handler_block) { ->(_, _, _) { raise 'It blew up!' } }

                it 'should count this work as retrying' do
                  subject.call(worker, job, nil) { raise error }
                  expect(resource.retrying).to eq(1)
                end
              end

              context 'when the error handler handles a different type of error' do
                let(:error_handler_class) { Interrupt }

                it 'should count this work as retrying' do
                  subject.call(worker, job, nil) { raise error }
                  expect(resource.retrying).to eq(1)
                end

                context 'when our error is a subclass of that error' do
                  let(:error_handler_class) { Exception }

                  it 'should NOT count this work as retrying' do
                    subject.call(worker, job, nil) { raise error }
                    expect(resource.retrying).to eq(0)
                  end
                end
              end
            end

            it 'should add this work back to the resource' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job).to include(original_job)
            end

            it 'should remove the job id before continuing' do
              subject.call(worker, job, nil) { raise error }
              expect(job).not_to include('jid')
            end

            it 'should add it for the original queue' do
              subject.call(worker, job, nil) { raise error }
              expect(error_queue).to eq(queue_name)
            end

            it 'should include serializable error information in the retrying job' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job['error']).to eq(SerializableError.new(error))
            end

            it 'should increment the number of times this error has been hit' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job['retry_count'][error.to_s]).to eq(1)
            end

            it 'should increment the total error count for this job' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job['retry_count']['total']).to eq(1)
            end

            shared_examples_for 'incrementing a retry count on a redis hash' do |prefix, name, key|
              it 'should increment the retry count' do
                subject.call(worker, job, nil) { raise error }
                expect(global_redis.hget("#{prefix}:#{public_send(name)}", key).to_i).to eq(1)
              end

              context 'with a previous count' do
                before { global_redis.hset("#{prefix}:#{public_send(name)}", key, 15) }

                it 'should increment the retry count' do
                  subject.call(worker, job, nil) { raise error }
                  expect(global_redis.hget("#{prefix}:#{public_send(name)}", key).to_i).to eq(16)
                end
              end
            end

            shared_examples_for 'incrementing retry counts for the queue and resource' do |key|
              it_behaves_like 'incrementing a retry count on a redis hash', 'resource:retry_count', :resource_name, key
              it_behaves_like 'incrementing a retry count on a redis hash', 'queue:retry_count', :queue_name, key
            end

            it_behaves_like 'incrementing retry counts for the queue and resource', 'total'

            describe 'saving the counts for the specific error' do
              let(:error) { StandardError.new('It broke!') }
              it_behaves_like 'incrementing retry counts for the queue and resource', 'It broke!'

              context 'with a different error' do
                let(:error) { StandardError.new('It blew up!') }
                it_behaves_like 'incrementing retry counts for the queue and resource', 'It blew up!'
              end
            end

            describe 'error logging' do
              let(:log_message) do
                "Retrying job '#{job['jid']}': #{error}\n#{error.backtrace * "\n"}"
              end

              it 'should log the error and backtrace to sidekiq' do
                expect(Sidekiq.logger).to receive(:warn).with(log_message)
                subject.call(worker, job, nil) { raise error }
              end

              context 'with no backtrace' do
                let(:error) { StandardError.new(Faker::Lorem.sentence) }
                let(:log_message) do
                  "Retrying job '#{job['jid']}': #{error}"
                end

                it 'should log the error to sidekiq' do
                  expect(Sidekiq.logger).to receive(:warn).with(log_message)
                  subject.call(worker, job, nil) { ThomasUtils::Future.error(error) }.get
                end
              end
            end

            context 'when this work cannot retry' do
              let(:retry_job) { false }

              it 'should raise the error' do
                expect { subject.call(worker, job, nil) { raise error }.get }.to raise_error(error)
              end

              context 'when the retry count is a number' do
                let(:retry_job) { 1 }

                it 'should not raise the error' do
                  expect { subject.call(worker, job, nil) { raise error }.get }.not_to raise_error
                end

                context 'when the number of retries has been exhausted' do
                  let(:total_retries) { 1 }

                  it 'should raise the error' do
                    expect { subject.call(worker, job, nil) { raise error }.get }.to raise_error(error)
                  end
                end
              end
            end

            context 'when the worker specified a sidekiq_retry_in_block' do
              let(:retry_count) { rand(1..10) }
              let(:worker_class) do
                Struct.new(:resource) do
                  include Worker
                  sidekiq_retry_in do |job_retry|
                    (job_retry.error_retries * job_retry.total_retries) * (job_retry.error.is_a?(StandardError) ? 2 : 7)
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
                job['retry_count'] ||= {}
                job['retry_count'][error.to_s] = retry_count
                subject.call(worker, job, nil) { raise error }
              end

              it 'should add this job to the retry set for the queue for this job' do
                expect(msg['queue']).to eq(queue_name)
              end

              it 'should add this job to the retry set for the resource for this job' do
                expect(msg['resource']).to eq(resource_name)
              end

              it 'should save the job' do
                expect(retry_msg_job).to include(original_job)
              end

              it 'should only retry the job once' do
                expect(resource.retrying).to eq(1)
              end

              it 'should retry the job in the time specified by the worker' do
                expect(global_redis.zscore('bolt:retry', serialized_msg)).to be_within(2).of(expected_retry_score)
              end

              context 'with a different error' do
                let(:error) { Interrupt.new }
                let(:expected_retry_score) { Time.now.to_f + 7 * (retry_count + 1) }

                it 'should retry the job in the time specified by the worker' do
                  expect(global_redis.zscore('bolt:retry', serialized_msg)).to be_within(2).of(expected_retry_score)
                end
              end
            end

            context 'when the worker specifies a sidekiq_freeze_resource_after_retry_for_block' do
              let(:original_job) { original_job_attributes }
              let(:borked) { false }
              let(:retry_count) { nil }
              let(:worker_class) do
                Struct.new(:resource) do
                  include Worker
                  sidekiq_freeze_resource_after_retry_for do |job_retry|
                    if job_retry.job['borked!'] == 'ice age'
                      :forever
                    elsif job_retry.error.is_a?(StandardError)
                      1
                    elsif job_retry.job['borked!']
                      7
                    elsif job_retry.resource_error_retries > 3 ||
                        job_retry.queue_error_retries > 3
                      9
                    elsif job_retry.error_retries.to_i > 10 ||
                        job_retry.resource_retries > 10 ||
                        job_retry.queue_retries > 10
                      13
                    end
                  end
                end
              end
              let(:frozen_resource) { global_redis.zrange('bolt:frozen_resource', 0, -1).first }
              let(:expected_defrost_time) { Time.now.to_f + 1 }
              let(:now) { Time.at(7777111111) }
              let(:resource_already_frozen) { false }
              let(:resource_retry_count) { 0 }
              let(:resource_error_retry_count) { 0 }
              let(:queue_retry_count) { 0 }
              let(:queue_error_retry_count) { 0 }

              around { |example| Timecop.freeze(now) { example.run } }

              before do
                job['borked!'] = borked
                job['retry_count'] ||= {}
                job['retry_count'][error.to_s] = retry_count
                resource.frozen = resource_already_frozen
                global_redis.hset("resource:retry_count:#{resource_name}", 'total', resource_retry_count)
                global_redis.hset("resource:retry_count:#{resource_name}", error.to_s, resource_error_retry_count)
                global_redis.hset("queue:retry_count:#{queue_name}", 'total', queue_retry_count)
                global_redis.hset("queue:retry_count:#{queue_name}", error.to_s, queue_error_retry_count)
                subject.call(worker, job, nil) { raise error }
              end

              it 'should freeze the resource' do
                expect(resource.frozen).to eq(true)
              end

              context 'when the resource is already frozen' do
                let(:resource_already_frozen) { true }

                it 'should not schedule this resource to be unfrozen later' do
                  expect(frozen_resource).to be_nil
                end
              end

              it 'should schedule this resource to be unfrozen later' do
                expect(frozen_resource).to eq(resource_name)
              end

              it 'should schedule the resource to be unfrozen at an interval specified by the worker' do
                expect(global_redis.zscore('bolt:frozen_resource', frozen_resource)).to eq(expected_defrost_time)
              end

              context 'with a different type of error' do
                let(:error) { Interrupt }

                it 'should not freeze the resource' do
                  expect(resource.frozen).to eq(false)
                end

                context 'when the job has something the worker does not like' do
                  let(:borked) { true }
                  let(:expected_defrost_time) { Time.now.to_f + 7 }

                  it 'should freeze the resource' do
                    expect(resource.frozen).to eq(true)
                  end

                  it 'should schedule the resource to be unfrozen at an interval specified by the worker' do
                    expect(global_redis.zscore('bolt:frozen_resource', frozen_resource)).to eq(expected_defrost_time)
                  end
                end

                shared_examples_for 'handling heavy retries' do |retry_key, amount|
                  let(retry_key) { 10 }
                  let(:expected_defrost_time) { Time.now.to_f + amount }

                  it 'should freeze the resource' do
                    expect(resource.frozen).to eq(true)
                  end

                  it 'should schedule the resource to be unfrozen at an interval specified by the worker' do
                    expect(global_redis.zscore('bolt:frozen_resource', frozen_resource)).to eq(expected_defrost_time)
                  end
                end

                context 'when retried too many times' do
                  it_behaves_like 'handling heavy retries', :retry_count, 13
                end

                context 'when the resource retried too many times' do
                  it_behaves_like 'handling heavy retries', :resource_retry_count, 13
                end

                context 'when the queue retried too many times' do
                  it_behaves_like 'handling heavy retries', :queue_retry_count, 13
                end

                context 'when the resource retried too many times' do
                  it_behaves_like 'handling heavy retries', :resource_error_retry_count, 9
                end

                context 'when the queue retried too many times' do
                  it_behaves_like 'handling heavy retries', :queue_error_retry_count, 9
                end

                context 'when the resource is not to be unfrozen' do
                  let(:borked) { 'ice age' }

                  it 'should freeze the resource' do
                    expect(resource.frozen).to eq(true)
                  end

                  it 'should not schedule this resource to be unfrozen later' do
                    expect(frozen_resource).to be_nil
                  end
                end
              end
            end

            context 'when the worker specifies a sidekiq_should_retry_block' do
              let(:original_job) { original_job_attributes }
              let(:worker_class) do
                Struct.new(:resource) do
                  include Worker
                  sidekiq_should_retry? do |job_retry|
                    !job_retry.error.is_a?(StandardError) && !job_retry.job['borked!'] && job_retry.error_retries.to_i < 10
                  end
                end
              end

              it 'should use that block to determine whether or not to retry' do
                expect { subject.call(worker, job, nil) { raise error }.get }.to raise_error(error)
              end

              context 'with a different error' do
                let(:error) { Interrupt.new }

                it 'should use that block to determine whether or not to retry' do
                  expect { subject.call(worker, job, nil) { raise error }.get }.not_to raise_error
                end
              end

              context 'when the job contains something that the worker does not like' do
                before { job['borked!'] = true }

                it 'should not retry' do
                  expect { subject.call(worker, job, nil) { raise error }.get }.to raise_error(error)
                end
              end

              context 'when the error count is used to determine whether or not to retry' do
                let(:error) { Interrupt.new }

                before do
                  job['retry_count'] ||= {}
                  job['retry_count'][error.to_s] = 9
                end

                it 'should not retry' do
                  expect { subject.call(worker, job, nil) { raise error }.get }.to raise_error(error)
                end
              end
            end
          end

        end

      end
    end
  end
end
