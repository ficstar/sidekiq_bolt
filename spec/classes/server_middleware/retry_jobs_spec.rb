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
          let(:job_id) { SecureRandom.uuid }
          let(:total_retries) { nil }
          let(:original_job) do
            {
                'queue' => queue_name,
                'resource' => resource_name,
                'retry' => retry_job,
                'jid' => job_id
            }
          end
          let(:job) { original_job.dup }

          before do
            original_job['retry_count:total'] = total_retries if total_retries
          end

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

            it 'should include the error information in the retrying job' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job['error']).to eq(error)
            end

            it 'should increment the number of times this error has been hit' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job["retry_count:#{error}"]).to eq(1)
            end

            it 'should increment the total error count for this job' do
              subject.call(worker, job, nil) { raise error }
              expect(error_job['retry_count:total']).to eq(1)
            end

            describe 'error logging' do
              let(:log_message) do
                "Retrying job '#{job['jid']}': #{error}\n#{error.backtrace * "\n"}"
              end

              it 'should log the error and backtrace to sidekiq' do
                expect(Sidekiq.logger).to receive(:warn).with(log_message)
                subject.call(worker, job, nil) { raise error }
              end
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
              let(:borked) { false }
              let(:retry_count) { nil }
              let(:worker_class) do
                Class.new do
                  include Worker
                  sidekiq_freeze_resource_after_retry_for do |job_retry|
                    if job_retry.job['borked!'] == 'ice age'
                      :forever
                    elsif job_retry.error.is_a?(StandardError)
                      1
                    elsif job_retry.job['borked!']
                      7
                    elsif job_retry.error_retries.to_i > 10
                      13
                    end
                  end
                end
              end
              let(:frozen_resource) { global_redis.zrange('bolt:frozen_resource', 0, -1).first }
              let(:expected_defrost_time) { Time.now.to_f + 1 }
              let(:now) { Time.at(7777111111) }
              let(:resource_already_frozen) { false }

              around { |example| Timecop.freeze(now) { example.run } }

              before do
                job['borked!'] = borked
                job["retry_count:#{error}"] = retry_count
                resource.frozen = resource_already_frozen
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

              context 'when the block returns nil' do
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

                context 'when retried too many times' do
                  let(:retry_count) { 10 }
                  let(:expected_defrost_time) { Time.now.to_f + 13 }

                  it 'should freeze the resource' do
                    expect(resource.frozen).to eq(true)
                  end

                  it 'should schedule the resource to be unfrozen at an interval specified by the worker' do
                    expect(global_redis.zscore('bolt:frozen_resource', frozen_resource)).to eq(expected_defrost_time)
                  end
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
              let(:worker_class) do
                Class.new do
                  include Worker
                  sidekiq_should_retry? do |job_retry|
                    !job_retry.error.is_a?(StandardError) && !job_retry.job['borked!'] && job_retry.error_retries.to_i < 10
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

                before { job["retry_count:#{error}"] = 9 }

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
