require 'rspec'

module Sidekiq
  module Bolt
    describe JobSuccession do

      describe '#call' do
        let(:parent_job_id) { SecureRandom.uuid }
        let(:job_id) { SecureRandom.uuid }
        #noinspection RubyStringKeysInHashInspection
        let(:job) { {'pjid' => parent_job_id, 'jid' => job_id} }
        let(:next_job) { {} }
        let(:block) { -> {} }
        let(:dependencies) { [job_id, SecureRandom.uuid] }

        before do
          dependencies.each { |jid| global_redis.sadd("dependencies:#{parent_job_id}", jid) }
          global_redis.set("parent:#{job_id}", parent_job_id)
        end

        it 'should yield' do
          expect { |block| subject.call(nil, job, nil, &block) }.to yield_control
        end

        shared_examples_for 'removing job dependencies' do
          let(:grandparent_job_id) { SecureRandom.uuid }

          before do
            global_redis.sadd("dependencies:#{grandparent_job_id}", parent_job_id)
            global_redis.set("parent:#{parent_job_id}", grandparent_job_id)
          end

          it 'should remove the parent job dependency' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.smembers("dependencies:#{parent_job_id}")).not_to include(job_id)
          end

          it 'should delete the parent key' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.get("parent:#{job_id}")).to be_nil
          end

          context 'when completing this job schedules other jobs' do
            let(:work) { SecureRandom.uuid }
            let(:queue_name) { Faker::Lorem.word }
            let(:resource_name) { Faker::Lorem.word }
            let(:resource) { Resource.new(resource_name) }
            let(:queue) { Queue.new(queue_name) }
            let(:scheduled_job) { {'queue' => queue_name, 'resource' => resource_name, 'work' => work} }
            let(:serialized_job) { JSON.dump(scheduled_job) }
            let(:result_allocation) { resource.allocate(1) }
            let(:result_queue) { result_allocation[0] }
            let(:result_work) { result_allocation[1] }

            before do
              global_redis.lpush("successive_work:#{job_id}", serialized_job)
              subject.call(nil, job, nil, &block) rescue nil
            end

            it 'should enqueue the scheduled work' do
              expect(result_work).to eq(work)
            end

            it 'should add it to the right queue' do
              expect(result_queue).to eq(queue_name)
            end

            it 'should create a link from the queue to the resource' do
              expect(queue.resources.map(&:name)).to include(resource_name)
            end

            it 'should keep a global reference of this resource' do
              expect(Resource.all.map(&:name)).to include(resource_name)
            end

            it 'should keep a global reference of this queue' do
              expect(Queue.all.map(&:name)).to include(queue_name)
            end
          end

          context 'when this job has child dependencies' do
            let(:child_job_id) { SecureRandom.uuid }

            before do
              global_redis.sadd("dependencies:#{job_id}", child_job_id)
            end

            it 'should not remove the parent job dependency' do
              subject.call(nil, job, nil, &block) rescue nil
              expect(global_redis.smembers("dependencies:#{parent_job_id}")).to include(job_id)
            end

            it 'should not delete the parent key' do
              subject.call(nil, job, nil, &block) rescue nil
              expect(global_redis.get("parent:#{job_id}")).not_to be_nil
            end
          end

          it 'should not remove the grand-parent job dependency' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.smembers("dependencies:#{grandparent_job_id}")).to include(parent_job_id)
          end

          it 'should not delete the grand-parent key' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.get("parent:#{parent_job_id}")).to eq(grandparent_job_id)
          end

          context 'when the parent job no longer has any dependencies' do
            let(:dependencies) { [job_id] }

            it 'should remove the grand-parent job dependency' do
              subject.call(nil, job, nil, &block) rescue nil
              expect(global_redis.smembers("dependencies:#{grandparent_job_id}")).not_to include(parent_job_id)
            end

            it 'should delete the grand-parent key' do
              subject.call(nil, job, nil, &block) rescue nil
              expect(global_redis.get("parent:#{parent_job_id}")).to be_nil
            end
          end
        end

        it_behaves_like 'removing job dependencies'

        context 'when the provided block raises an error' do
          let(:block) { ->() { raise 'It broke!' } }

          it_behaves_like 'removing job dependencies'

          it 'should re-raise the error' do
            expect { subject.call(nil, job, nil, &block) }.to raise_error
          end
        end

      end

    end
  end
end
