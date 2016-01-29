require 'rspec'

module Sidekiq
  module Bolt
    describe ProcessSweeper do

      let(:process) { SecureRandom.uuid }

      subject { ProcessSweeper.new(process) }

      before { global_redis.sadd('bolt:processes', process) }

      describe '#sweep' do
        let(:queue_name) { Faker::Lorem.word }
        let(:resource_name) { Faker::Lorem.word }
        let(:work) { SecureRandom.uuid }
        let(:backup_work) { {'queue' => queue_name, 'resource' => resource_name, 'work' => work, } }
        let(:serialized_backup_work) { JSON.dump(backup_work) }
        let(:resource) { Resource.new(resource_name) }
        let(:result_allocation) { resource.allocate(1) }
        let(:result_queue) { result_allocation[0] }
        let(:result_work) { result_allocation[1] }

        before do
          global_redis.lpush("resource:backup:worker:#{process}", serialized_backup_work)
        end

        it 'should remove the process from the processes set' do
          subject.sweep
          expect(global_redis.smembers('bolt:processes')).not_to include(process)
        end

        xit 'should re-insert the work back into the queue' do
          subject.sweep
          expect(result_work).to eq(work)
        end

        xit 'should re-insert into the right queue' do
          subject.sweep
          expect(result_queue).to eq(queue_name)
        end
      end

    end
  end
end
