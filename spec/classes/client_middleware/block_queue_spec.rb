require 'rspec'

module Sidekiq
  module Bolt
    module ClientMiddleware
      describe BlockQueue do

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:queue) { Queue.new(queue_name) }
          let(:parent_job_id) { SecureRandom.base64 }
          let(:job) { {'queue' => queue_name, 'pjid' => parent_job_id} }
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

          shared_examples_for 'preventing a job from running' do
            it 'should not yield' do
              expect { |block| subject.call(nil, job, nil, &block) }.not_to yield_control
            end

            it 'should return false' do
              expect(subject.call(nil, job, nil, nil) { yield_result }).to eq(false)
            end
          end

          context 'when the queue is blocked' do
            before { queue.blocked = true }
            it_behaves_like 'preventing a job from running'
          end

          context 'when a parent job has completed' do
            before { global_redis.set("job_completed:#{parent_job_id}", 'true') }

            it 'should not yield' do
              expect { |block| subject.call(nil, job, nil, &block) rescue nil }.not_to yield_control
            end

            it 'should raise an error indicating that this is an invalid state' do
              expect { subject.call(nil, job, nil, nil) { yield_result } }.to raise_error('Cannot add job dependency to an already completed job!')
            end
          end

          context 'when a parent job has failed' do
            before { global_redis.set("job_failed:#{parent_job_id}", 'true') }
            it_behaves_like 'preventing a job from running'
          end
        end

      end
    end
  end
end
