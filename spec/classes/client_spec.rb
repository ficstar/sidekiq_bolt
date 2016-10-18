require 'rspec'

module Sidekiq
  module Bolt
    describe Client do
      class MockWorker
        def perform(*_)
        end
      end

      let(:queue_name) { Faker::Lorem.word }
      let(:resource_name) { Faker::Lorem.word }
      let(:resource_type) { nil }
      let(:klass) { Faker::Lorem.word }
      let(:args) { Faker::Lorem.paragraphs }
      let(:at) { nil }
      let(:error) { nil }
      #noinspection RubyStringKeysInHashInspection
      let(:item) do
        {
            'queue' => queue_name,
            'resource' => resource_name,
            'class' => klass,
            'args' => args,
            'at' => at,
            'error' => error
        }
      end
      let!(:original_item) { item.dup }
      let(:resource) { Resource.new(resource_name) }
      let(:result_work) { resource.allocate(1) }
      let(:result_queue) { result_work[0] }
      let(:result_item) { Sidekiq.load_json(result_work[2]) if result_work[2] }
      let(:worker_id) { SecureRandom.uuid }
      let(:sidekiq_options) { {concurrency: 0} }

      before do
        resource.type = resource_type
        allow_any_instance_of(Client).to receive(:identity).and_return(worker_id)
      end

      it { is_expected.to be_a_kind_of(Sidekiq::Client) }
      it { is_expected.to be_a_kind_of(Sidekiq::Util) }

      describe '#skeleton_push' do
        it 'should push the item on to the queue for the specified resource' do
          subject.skeleton_push(item)
          expect(result_item).to include(original_item)
        end

        shared_examples_for 'backing up local work' do
          let(:queue) { Queue.new(queue_name) }
          let(:backup_work_key) { "resource:backup:worker:#{worker_id}" }
          let(:serialized_backup_work) { global_redis.lrange(backup_work_key, 0, -1).first }
          let(:backup_item) { JSON.load(serialized_backup_work) }
          let(:backup_queue) { backup_item['queue'] }
          let(:backup_resource) { backup_item['resource'] }
          let(:backup_work) { Sidekiq.load_json(backup_item['work']) }

          it 'should not push the item on to the queue' do
            subject.skeleton_push(item)
            expect(result_item).to be_nil
          end

          it 'should back up the work' do
            subject.skeleton_push(item)
            expect(backup_work).to include(original_item)
          end

          it 'should increment the resource allocation' do
            subject.skeleton_push(item)
            expect(resource.allocated).to eq(1)
          end

          it 'should increment the queue busy count' do
            subject.skeleton_push(item)
            expect(queue.busy).to eq(1)
          end
        end

        context 'when the work is run locally' do
          let(:klass) { MockWorker.to_s }
          let(:resource_name) { Resource::ASYNC_LOCAL_RESOURCE }

          it_behaves_like 'backing up local work'

          describe 'running the job' do
            it 'should create and run the worker within the server middleware' do
              expect(Sidekiq.server_middleware).to receive(:invoke).with(a_kind_of(MockWorker), item, item['queue']) do |&block|
                expect_any_instance_of(MockWorker).to receive(:perform).with(*args)
                block.call
              end
              subject.skeleton_push(item)
            end

            context 'when a Sidekiq client' do
              before { allow(Sidekiq).to receive(:server?).and_return(false) }

              it 'should push the item on to the queue' do
                subject.skeleton_push(item)
                global_redis.set("resource:allocated:#{resource_name}", 0)
                expect(result_item).not_to be_nil
              end
            end
          end
        end

        context 'when we have local workers available to perform this work' do
          let(:resource_type) { Faker::Lorem.sentence }
          let(:sidekiq_options) { {concurrency: 1, concurrency_pool: {resource_type => 1}} }
          let(:resource_limit) { nil }
          let(:klass) { MockWorker.to_s }
          let(:expected_work) do
            Fetch::UnitOfWork.new(queue_name, '-1', resource_name, Sidekiq.dump_json(item), resource_type)
          end

          before { resource.limit = resource_limit }

          it_behaves_like 'backing up local work'

          it 'should allocate a worker' do
            subject.skeleton_push(item)
            expect(Fetch.processor_allocator.allocation(resource_type)).to eq(1)
          end

          it 'should add a UnitOfWork to the local_queue of the Fetch' do
            subject.skeleton_push(item)
            work = (Fetch.local_queue.pop unless Fetch.local_queue.empty?)
            expect(work).to eq(expected_work)
          end

          context 'when the resource has a limit' do
            let(:resource_limit) { 3 }
            let(:queue) { Queue.new(queue_name) }
            let(:busy_queue_name) { Faker::Lorem.sentence }
            let(:backup_work_key) { "resource:backup:worker:#{worker_id}" }
            let(:serialized_backup_work) { global_redis.lrange(backup_work_key, 0, -1).first }
            let(:resource) { Resource.new(resource_name) }

            before do
              resource_limit.times { resource.add_work(busy_queue_name, SecureRandom.base64, false) }
              resource.allocate(resource_allocation)
            end

            context 'when some room is left' do
              let(:resource_allocation) { 2 }
              let(:expected_work) do
                Fetch::UnitOfWork.new(queue_name, '3', resource_name, Sidekiq.dump_json(item), resource_type)
              end

              it 'should add a UnitOfWork to the local_queue of the Fetch' do
                subject.skeleton_push(item)
                work = (Fetch.local_queue.pop unless Fetch.local_queue.empty?)
                expect(work).to eq(expected_work)
              end

              it 'should remove the allocation' do
                subject.skeleton_push(item)
                expect(resource.allocations_left).to eq(0)
              end
            end

            context 'when the resource limit has been reached' do
              let(:resource_allocation) { 3 }

              it 'should push the item on to the queue' do
                subject.skeleton_push(item)
                resource.limit += 1
                expect(result_item).not_to be_nil
              end

              it 'should not back up the work' do
                subject.skeleton_push(item)
                expect(serialized_backup_work).to be_nil
              end

              it 'should not increment the resource allocation' do
                subject.skeleton_push(item)
                expect(resource.allocated).to eq(resource_limit)
              end

              it 'should not increment the queue busy count' do
                subject.skeleton_push(item)
                expect(queue.busy).to eq(0)
              end

              it 'should not allocate a worker' do
                subject.skeleton_push(item)
                expect(Fetch.processor_allocator.allocation(resource_name)).to eq(0)
              end

              it 'should not locally schedule any work' do
                subject.skeleton_push(item)
                work = (Fetch.local_queue.pop unless Fetch.local_queue.empty?)
                expect(work).to be_nil
              end
            end
          end
        end
      end

      describe 'push items into the queue' do
        let(:now) { Time.now }

        around { |example| Timecop.freeze(now) { example.run } }

        it 'should push the item on to the queue for the specified resource' do
          subject.push(item)
          expect(result_item).to include(original_item)
        end

        it 'should not use a redis multi' do
          expect_any_instance_of(Redis).not_to receive(:multi)
          subject.push(item)
        end

        it 'should include the time the item was enqueued at' do
          subject.push(item)
          expect(result_item).to include('enqueued_at' => now.to_f)
        end

        it 'should use the right queue' do
          subject.push(item)
          expect(result_queue).to eq(queue_name)
        end

        it 'should not add this item to the retrying queue' do
          subject.push(item)
          expect(resource.retrying).to eq(0)
        end

        context 'when the job is retrying' do
          let(:error) { 'It blew up!' }

          it 'should add this item to the retrying queue' do
            subject.push(item)
            expect(resource.retrying).to eq(1)
          end
        end

        context 'when the item is scheduled for later' do
          let(:at) { (now + 120).to_f }
          let(:result_msg) { global_redis.zrange('schedule', 0, -1).first }
          let(:result_item) { Sidekiq.load_json(result_msg) }

          it 'should add the message to the schedule set' do
            subject.push(item)
            expect(result_item).to include(original_item.except('at'))
          end

          it 'should include a key indicating that it came from a Bolt client' do
            subject.push(item)
            expect(result_item).to include('sk' => 'bolt')
          end
        end

        describe 'pushing multiple items' do
          let(:args_two) { Faker::Lorem.paragraphs }
          let!(:original_item_two) { item.dup }
          let(:result_work) { resource.allocate(2) }
          let(:result_queue_two) { result_work[2] }
          let(:result_item_two) { Sidekiq::load_json(result_work[5]) }
          let(:items) { {'queue' => queue_name, 'resource' => resource_name, 'class' => klass, 'args' => [args, args_two]} }
          let(:result_args) { [result_item['args'], result_item_two['args']] }

          it 'should push each item on to the queue' do
            subject.push_bulk(items)
            expect(result_args).to match_array([args, args_two])
          end
        end

        context 'with no resource specified' do
          let(:resource_name) { nil }
          let(:resource) { Resource.new('default') }

          it 'should push the item on to the queue for the default resource' do
            subject.push(item)
            expect(result_item).to include(original_item.merge('resource' => 'default'))
          end
        end
      end

      describe 'pushing using Sidekiq::Client.push' do
        before { Sidekiq::Client.push(item) }

        it 'should use the default Sidekiq behaviour' do
          result_msg = global_redis.rpop("queue:#{queue_name}")
          result_item = Sidekiq.load_json(result_msg)
          expect(result_item).to include(original_item)
        end

        context 'when the item came from a Bolt Client' do
          let(:item) { {'queue' => queue_name, 'resource' => resource_name, 'class' => klass, 'args' => args, 'at' => at, 'sk' => 'bolt'} }

          it 'should push the item using a Bolt Client' do
            expect(result_item).to include(original_item)
          end
        end
      end

    end
  end
end
