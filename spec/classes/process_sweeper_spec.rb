require 'rspec'

module Sidekiq
  module Bolt
    describe ProcessSweeper do

      let(:process) { SecureRandom.uuid }

      subject { ProcessSweeper.new(process) }

      before { global_redis.sadd('bolt:processes', process) }

      describe '#sweep' do
        let(:queue_name) { Faker::Lorem.word }
        let(:queue) { Queue.new(queue_name) }
        let(:resource_name) { Faker::Lorem.word }
        let(:resource) { Resource.new(resource_name) }
        let(:work) { SecureRandom.uuid }
        let(:backup_work) { {'queue' => queue_name, 'resource' => resource_name, 'work' => work, 'allocation' => '1'} }
        let(:serialized_backup_work) { JSON.dump(backup_work) }
        let(:result_allocation) { resource.allocate(1) }
        let(:result_queue) { result_allocation[0] }
        let(:result_work) { result_allocation[2] }
        let(:limit) { nil }

        before do
          resource.limit = limit
          global_redis.lpush("resource:backup:worker:#{process}", serialized_backup_work)
          resource.add_work(queue_name, work)
          resource.allocate(1)
        end

        it 'should remove the process from the processes set' do
          subject.sweep
          expect(global_redis.smembers('bolt:processes')).not_to include(process)
        end

        it 'should re-insert the work back into the queue' do
          subject.sweep
          expect(result_work).to eq(work)
        end

        it 'should re-insert into the right queue' do
          subject.sweep
          expect(result_queue).to eq(queue_name)
        end

        context 'when the resource has a limit' do
          let(:limit) { 3 }

          it 'should return the resource' do
            subject.sweep
            expect(resource.allocations_left).to eq(3)
          end
        end

        context 'with more backed up work' do
          let(:queue_name_two) { Faker::Lorem.word }
          let(:backup_work_two) { {'queue' => queue_name_two, 'resource' => resource_name, 'work' => work, } }
          let(:serialized_backup_work_two) { JSON.dump(backup_work_two) }
          let(:result_allocation_two) { resource.allocate(1) }
          let(:result_queue_two) { result_allocation[0] }
          let(:result_work_two) { result_allocation[1] }

          before do
            global_redis.lpush("resource:backup:worker:#{process}", serialized_backup_work_two)
          end

          it 'should re-insert the work back into the queue' do
            subject.sweep
            expect(result_work).to eq(work)
          end

          it 'should re-insert into the right queue' do
            subject.sweep
            expect(result_queue).to eq(queue_name)
          end

          it 'should remove all work from the queue' do
            subject.sweep
            expect(global_redis.lrange("resource:backup:worker:#{process}", 0, -1)).to be_empty
          end
        end

        it 'should count the work as retrying' do
          subject.sweep
          expect(resource.retrying).to eq(1)
        end

        it 'should decrement the resource allocation count' do
          subject.sweep
          expect(resource.allocated).to eq(0)
        end

        it 'should decrement the queue busy count' do
          subject.sweep
          expect(queue.busy).to eq(0)
        end

        context 'when the worker has allocated persistent resources' do
          let(:persistent_item) { SecureRandom.uuid }
          let(:list_of_persistent_items) { [persistent_item] }
          let(:persistent_resource_name) { SecureRandom.uuid }
          let(:persistent_resource) { PersistentResource.new(persistent_resource_name) }

          before do
            allow(persistent_resource).to receive(:identity).and_return(process)
            list_of_persistent_items.each do |item|
              persistent_resource.create(item)
              persistent_resource.allocate
            end
          end

          it 'should add the item back into the pool with a really high score' do
            subject.sweep
            expect(global_redis.zrangebyscore("resources:persistent:#{persistent_resource_name}", 0.0, 0.0)).to include(persistent_item)
          end

          it 'should clear the backup list' do
            subject.sweep
            expect(global_redis.lrange("resources:persistent:backup:worker:#{process}", 0, -1)).to be_empty
          end

          context 'with multiple items allocated' do
            let(:persistent_item_two) { SecureRandom.uuid }
            let(:list_of_persistent_items) { [persistent_item, persistent_item_two] }

            it 'should add the first item' do
              subject.sweep
              expect(global_redis.zrangebyscore("resources:persistent:#{persistent_resource_name}", 0.0, 0.0)).to include(persistent_item)
            end

            it 'should add the second item' do
              subject.sweep
              expect(global_redis.zrangebyscore("resources:persistent:#{persistent_resource_name}", 0.0, 0.0)).to include(persistent_item_two)
            end

            it 'should clear the backup list' do
              subject.sweep
              expect(global_redis.lrange("resources:persistent:backup:worker:#{process}", 0, -1)).to be_empty
            end
          end

        end
      end

    end
  end
end
