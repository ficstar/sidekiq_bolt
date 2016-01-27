require 'rspec'

module Sidekiq
  module Bolt
    module ClientMiddleware
      describe JobSuccession do

        describe '#call' do
          let(:job_id) { SecureRandom.uuid }
          let(:parent_job_id) { SecureRandom.uuid }
          let(:error) { nil }
          #noinspection RubyStringKeysInHashInspection
          let(:job) { {'pjid' => parent_job_id, 'jid' => job_id, 'error' => error} }
          let(:yield_result) { Faker::Lorem.word }

          it 'should yield' do
            expect { |block| subject.call(nil, job, nil, &block) }.to yield_control
          end

          it 'should support an optional redis pool' do
            expect { subject.call(nil, job, nil, nil) {} }.not_to raise_error
          end

          it 'should return the value of the block' do
            expect(subject.call(nil, job, nil) { yield_result }).to eq(yield_result)
          end

          it 'should create a dependency link to the parent job' do
            subject.call(nil, job, nil) {}
            expect(global_redis.smembers("dependencies:#{parent_job_id}")).to include(job_id)
          end

          it 'should create a dependency on its own completion' do
            subject.call(nil, job, nil) {}
            expect(global_redis.smembers("dependencies:#{job_id}")).to include(job_id)
          end

          it 'should create a reference to the parent job' do
            subject.call(nil, job, nil) {}
            expect(global_redis.get("parent:#{job_id}")).to eq(parent_job_id)
          end

          it 'should ensure that the parent job is not considered complete' do
            global_redis.set("job_completed:#{parent_job_id}", 'true')
            subject.call(nil, job, nil) {}
            expect(global_redis.get("job_completed:#{parent_job_id}")).to be_nil
          end

          it 'should ensure that this job is not considered complete' do
            global_redis.set("job_completed:#{job_id}", 'true')
            subject.call(nil, job, nil) {}
            expect(global_redis.get("job_completed:#{job_id}")).to be_nil
          end

          context 'when this job is retrying' do
            let(:error) { SecureRandom.uuid }

            it 'should not create a dependency link to the parent job' do
              subject.call(nil, job, nil) {}
              expect(global_redis.smembers("dependencies:#{parent_job_id}")).not_to include(job_id)
            end
          end

          context 'when a dependency has already been created for this job' do
            let(:prev_parent_job_id) { parent_job_id }

            before { global_redis.set("parent:#{job_id}", prev_parent_job_id) }

            it 'should not raise an error' do
              expect { subject.call(nil, job, nil) {} }.not_to raise_error
            end

            context 'when the parent job is different from the new one' do
              let(:prev_parent_job_id) { SecureRandom.uuid }

              it 'should raise an error' do
                expect { subject.call(nil, job, nil) {} }.to raise_error(JobSuccession::DuplicateJobError, "Attempted to enqueue duplicate job '#{job_id}' with parent '#{parent_job_id}'")
              end
            end
          end

        end

      end
    end
  end
end
