require 'rspec'

module Sidekiq
  module Bolt
    describe Resource do

      let(:name) { 'resourceful' }

      subject { Resource.new(name) }

      it { is_expected.to be_a_kind_of(Sidekiq::Util) }

      describe '.all' do
        let(:resource_names) { %w(res1 res2 res3) }
        let(:resources) { resource_names.map { |name| Resource.new(name) } }

        subject { Resource.all }

        before { resources.each { |resource| resource.add_work('queue', '') } }

        it { is_expected.to match_array(resources) }

        context 'with a different list of resources' do
          let(:resource_names) { %w(heav_resource light_weight resourceful) }

          it { is_expected.to match_array(resources) }
        end
      end

      describe '.having_types' do
        let(:supported_types) { %w(light) }
        let(:resource_types) { %w(light medium heavy) }
        let!(:resources) do
          resource_types.map do |type|
            Resource.new(Faker::Lorem.sentence).tap { |resource| resource.type = type if type }
          end
        end

        subject { Resource.having_types(supported_types) }

        before { resources.each { |resource| resource.add_work('queue', '') } }

        it { is_expected.to match_array([resources[0]]) }

        context 'with a different list of supported types' do
          let(:supported_types) { %w(heavy) }

          it { is_expected.to match_array([resources[2]]) }
        end

        context 'with a multiple supported types' do
          let(:supported_types) { %w(medium heavy) }

          it { is_expected.to match_array(resources[1..2]) }
        end

        context 'when the resource does not have a type' do
          let(:supported_types) { %w(default) }
          let(:resource_types) { [nil] }

          it 'should treat this as a default resource type' do
            is_expected.to eq(resources)
          end
        end
      end

      describe '.workers_required' do
        let(:resource_name) { Faker::Lorem.word }
        let(:resource_limit) { 15 }
        let(:resource_type) { Faker::Lorem.word }
        let!(:resource) do
          Resource.new(resource_name).tap do |resource|
            resource.type = resource_type
            resource.limit = resource_limit
            resource.add_work(Faker::Lorem.word, Faker::Lorem.word)
          end
        end

        subject { Resource.workers_required }

        it { is_expected.to eq(resource_type => 15) }

        context 'with unlimited bandwidth' do
          let(:resource_limit) { nil }
          it { is_expected.to eq(resource_type => 1/0.0) }
        end

        context 'with another resource' do
          let(:resource_name_two) { Faker::Lorem.sentence }
          let(:resource_type_two) { Faker::Lorem.word }
          let!(:resource_two) do
            Resource.new(resource_name_two).tap do |resource|
              resource.type = resource_type_two
              resource.limit = 77
              resource.add_work(Faker::Lorem.word, Faker::Lorem.word)
            end
          end

          it { is_expected.to eq(resource_type => 15, resource_type_two => 77) }

          context 'when they share the same type' do
            let(:resource_type_two) { resource_type }

            it { is_expected.to eq(resource_type => 92) }
          end
        end
      end

      describe '#type' do
        let(:type) { 'zippers' }

        before { subject.type = type }

        it 'should be the specified type' do
          expect(subject.type).to eq('zippers')
        end

        it 'should store the value in redis' do
          expect(global_redis.get('resource:type:resourceful')).to eq('zippers')
        end
      end

      describe '#limit' do
        let(:limit) { rand(1..10) }
        let(:expected_pool) { (1..limit).to_a }
        let(:name) { Faker::Lorem.sentence }

        before { subject.limit = limit }

        it 'should be the specified limit' do
          expect(subject.limit).to eq(limit)
        end

        it 'should store the value in redis' do
          expect(global_redis.get("resource:limit:#{name}").to_i).to eq(limit)
        end

        it 'should create a reference pool of allocations' do
          expect(global_redis.smembers("resource:pool:reference:#{name}").map(&:to_i)).to match_array(expected_pool)
        end

        it 'should create a pool of allocations' do
          expect(global_redis.zrange("resource:pool:#{name}", 0, -1).map(&:to_i)).to match_array(expected_pool)
        end

        context 'when we have a limit and then remove it' do
          let(:limit) { 5 }
          let(:limit_two) { nil }

          before { subject.limit = limit_two }

          it 'should remove the limit' do
            expect(global_redis.get("resource:limit:#{name}")).to be_nil
          end

          it 'should empty the reference pool' do
            expect(global_redis.smembers("resource:pool:reference:#{name}")).to be_empty
          end

          it 'should empty the allocation pool' do
            expect(global_redis.zrange("resource:pool:#{name}", 0, -1)).to be_empty
          end
        end

        context 'when a lower previous limit has been set and items have been already allocated' do
          let(:limit) { 5 }
          let(:limit_two) { 6 }
          let!(:allocated) do
            global_redis.zrangebyscore("resource:pool:#{name}", '-inf', 'inf', limit: [0, 1]).first.tap do |item|
              global_redis.zrem("resource:pool:#{name}", item)
            end.to_i
          end

          before { subject.limit = limit_two }

          it 'should not include the allocated item in the pool' do
            expect(global_redis.zrange("resource:pool:#{name}", 0, -1).map(&:to_i)).not_to include(allocated)
          end
        end

        context 'when a higher previous limit has been set' do
          let(:limit) { 10 }
          let(:limit_two) { 5 }
          let(:expected_pool) { (1..limit_two).to_a }

          before { subject.limit = limit_two }

          it 'should remove the items from the reference pool' do
            expect(global_redis.smembers("resource:pool:reference:#{name}").map(&:to_i)).to match_array(expected_pool)
          end

          it 'should remove the items from the pool' do
            expect(global_redis.zrange("resource:pool:#{name}", 0, -1).map(&:to_i)).to match_array(expected_pool)
          end
        end

        it 'should be an integer property' do
          expect(Resource.new(name).limit).to eq(limit)
        end
      end

      describe '#frozen=' do
        let(:frozen) { false }

        before do
          subject.frozen = true
          subject.frozen = frozen
        end

        it 'should store the value in redis' do
          expect(!!global_redis.get('resource:frozen:resourceful')).to eq(false)
        end

        context 'when frozen' do
          let(:frozen) { true }

          it 'should store the value in redis' do
            expect(!!global_redis.get('resource:frozen:resourceful')).to eq(true)
          end
        end

        context 'with a different resource' do
          let(:name) { 'resource-ified' }

          it 'should store the value in redis' do
            expect(!!global_redis.get('resource:frozen:resource-ified')).to eq(false)
          end

          context 'when frozen' do
            let(:frozen) { true }

            it 'should store the value in redis' do
              expect(!!global_redis.get('resource:frozen:resource-ified')).to eq(true)
            end
          end
        end
      end

      describe '#frozen' do
        let(:frozen) { false }

        subject { Resource.new(name).frozen }

        before { Resource.new(name).frozen = frozen }

        it { is_expected.to eq(false) }

        context 'when frozen' do
          let(:frozen) { true }

          it { is_expected.to eq(true) }
        end

        context 'with a different resource' do
          let(:name) { 'resource-ified' }

          it { is_expected.to eq(false) }

          context 'when frozen' do
            let(:frozen) { true }

            it { is_expected.to eq(true) }
          end
        end
      end

      describe '#allocated' do
        its(:allocated) { is_expected.to eq(0) }

        context 'when this resource is in use' do
          let(:busy) { 5 }

          before { global_redis.set("resource:allocated:#{name}", busy) }

          its(:allocated) { is_expected.to eq(5) }

          context 'with a different resource' do
            let(:busy) { 15 }
            let(:name) { 'really_busy' }

            its(:allocated) { is_expected.to eq(15) }
          end
        end
      end

      describe 'allocations_left' do
        let(:name) { Faker::Lorem.sentence }

        its(:allocations_left) { is_expected.to eq(-1) }

        context 'with a limit' do
          let(:limit) { rand(5..10) }
          let(:fetched_count) { rand(1...limit) }

          before do
            subject.limit = limit
            items = global_redis.zrangebyscore("resource:pool:#{name}", '-inf', 'inf', limit: [0, fetched_count])
            global_redis.pipelined { items.each { |allocation| global_redis.zrem("resource:pool:#{name}", allocation) } }
          end

          its(:allocations_left) { is_expected.to eq(limit - fetched_count) }
        end
      end

      describe '#over_allocated' do
        let(:name) { 'disaster' }

        its(:over_allocated) { is_expected.to eq(0) }

        context 'when this resource has been overworked' do
          let(:busy) { 77 }

          before { global_redis.set("resource:over-allocated:#{name}", busy) }

          its(:over_allocated) { is_expected.to eq(77) }

          context 'with a different resource' do
            let(:busy) { 127 }
            let(:name) { 'total-disaster' }

            its(:over_allocated) { is_expected.to eq(127) }
          end
        end
      end

      describe '#add_work' do
        let(:work) { 'piece_of_work' }
        let(:queue) { 'workload' }

        it 'should add the new work to the specified queue' do
          subject.add_work(queue, work)
          expect(global_redis.rpop('resource:queue:workload:resourceful')).to eq('piece_of_work')
        end

        context 'when the queue name is actually a queue object' do
          let(:queue_name) { Faker::Lorem.word }
          let(:queue) { Queue.new(queue_name) }

          it 'should use the name of the queue' do
            subject.add_work(queue, work)
            expect(global_redis.rpop("resource:queue:#{queue_name}:resourceful")).to eq('piece_of_work')
          end
        end

        context 'when the work is consider retrying' do
          let(:work) { SecureRandom.uuid }
          let(:name) { Faker::Lorem.word }
          let(:queue) { Faker::Lorem.word }

          it 'should add the new work to the resource retry queue' do
            subject.add_work(queue, work, true)
            expect(global_redis.rpop("resource:queue:retrying:#{queue}:#{name}")).to eq(work)
          end
        end

        it 'should associate the queue with the resource' do
          subject.add_work(queue, work)
          expect(global_redis.smembers('resource:queues:resourceful')).to include('workload')
        end

        it 'should associate the resource with the queue' do
          subject.add_work(queue, work)
          expect(global_redis.smembers('queue:resources:workload')).to include('resourceful')
        end

        it 'should store the resource in the global list' do
          subject.add_work(queue, work)
          expect(global_redis.smembers('resources')).to include('resourceful')
        end

        it 'should store the queue in the global list' do
          subject.add_work(queue, work)
          expect(global_redis.smembers('queues')).to include('workload')
        end

        context 'with a different resource and queue' do
          let(:name) { 'heavy_duty' }
          let(:work) { 'hard_work' }
          let(:queue) { 'heavy_workload' }

          it 'should add the new work to the specified queue' do
            subject.add_work(queue, work)
            expect(global_redis.rpop('resource:queue:heavy_workload:heavy_duty')).to eq('hard_work')
          end

          it 'should associate the queue with the resource' do
            subject.add_work(queue, work)
            expect(global_redis.smembers('resource:queues:heavy_duty')).to include('heavy_workload')
          end

          it 'should associate the resource with the queue' do
            subject.add_work(queue, work)
            expect(global_redis.smembers('queue:resources:heavy_workload')).to include('heavy_duty')
          end

          it 'should store the resource in the global list' do
            subject.add_work(queue, work)
            expect(global_redis.smembers('resources')).to include('heavy_duty')
          end

          it 'should store the queue in the global list' do
            subject.add_work(queue, work)
            expect(global_redis.smembers('queues')).to include('heavy_workload')
          end
        end

        context 'when called multiple times' do
          it 'should add the new work to the queue from the left' do
            subject.add_work(queue, work)
            subject.add_work(queue, 'other_work')
            expect(global_redis.rpop('resource:queue:workload:resourceful')).to eq('piece_of_work')
          end
        end
      end

      describe '#queues' do
        let(:queues) { Faker::Lorem.sentences }

        before do
          queues.each { |queue| subject.add_work(queue, SecureRandom.uuid) }
        end

        it 'should return the list of queue names associated with this resource' do
          expect(subject.queues).to match_array(queues)
        end

        context 'when filtering by group' do
          let(:queue_group) { Faker::Lorem.sentence }
          let(:grouped_queues) { queues.sample(3) }

          before do
            grouped_queues.each { |queue| Queue.new(queue).group = queue_group }
          end

          it 'should filter the list of queues by the specified group' do
            expect(subject.queues(queue_group)).to match_array(grouped_queues)
          end

          context 'when the queues belong to the "any" group' do
            before do
              queues.each { |queue| Queue.new(queue).group = '__ANY__' }
            end

            it 'should retrieve all queues' do
              expect(subject.queues(queue_group)).to match_array(queues)
            end
          end
        end
      end

      describe '#size_for_queue' do
        let(:queue) { SecureRandom.base64 }
        let(:counted_queue) { queue }
        let(:work) { Faker::Lorem.words(4) }
        let(:resource) { Resource.new(name) }

        subject { resource.size_for_queue(counted_queue) }

        before { work.each { |item| resource.add_work(queue, item) } }

        it { is_expected.to eq(4) }

        context 'with multiple queues' do
          let(:queue_two) { SecureRandom.base64 }
          let(:work_two) { Faker::Lorem.words(7) }

          before { work_two.each { |item| resource.add_work(queue_two, item) } }

          it { is_expected.to eq(4) }

          context 'with the other queue' do
            let(:counted_queue) { queue_two }

            it { is_expected.to eq(7) }
          end
        end

        context 'when the request queue is a Queue object' do
          let(:counted_queue) { Queue.new(queue) }

          it { is_expected.to eq(4) }
        end
      end

      shared_examples_for 'counting work in queues' do |method, retrying|
        let(:queues) { %w(queue1 queue2) }
        let(:work) { 2 }

        before do
          queues.each { |queue| work.times { subject.add_work(queue, SecureRandom.uuid, retrying) } }
        end

        its(method) { is_expected.to eq(4) }

        context 'with no queues' do
          let(:queues) { [] }

          its(method) { is_expected.to eq(0) }
        end

        context 'with a different resource' do
          let(:name) { 'heavy_worker' }
          let(:queues) { %w(big_queue1 big_queue2 big_queue3) }

          its(method) { is_expected.to eq(6) }
        end

        context 'with a different amount of work' do
          let(:work) { 4 }

          its(method) { is_expected.to eq(8) }
        end
      end

      describe '#size' do
        it_behaves_like 'counting work in queues', :size, false
      end

      describe '#retrying' do
        it_behaves_like 'counting work in queues', :retrying, true

        context 'when the retry comes from the retry set' do
          let(:queue_name) { Faker::Lorem.word }
          let(:serialized_msg) do
            JSON.dump('queue' => queue_name, 'resource' => name)
          end

          before do
            global_redis.zadd('bolt:retry', Time.now.to_f, serialized_msg)
          end

          its(:retrying) { is_expected.to eq(1) }

          context 'with multiple retrying items' do
            let(:queue_name_two) { Faker::Lorem.sentence }
            let(:serialized_msg_two) do
              JSON.dump('queue' => queue_name_two, 'resource' => name)
            end

            before do
              global_redis.zadd('bolt:retry', Time.now.to_f, serialized_msg_two)
            end

            its(:retrying) { is_expected.to eq(2) }
          end

          context 'with a different resource' do
            let(:resource_two) { Faker::Lorem.word }
            let(:serialized_msg_two) do
              JSON.dump('queue' => queue_name, 'resource' => resource_two)
            end

            before do
              global_redis.zadd('bolt:retry', Time.now.to_f, serialized_msg_two)
            end

            its(:retrying) { is_expected.to eq(1) }
          end
        end
      end

      describe '#allocate' do
        let(:retrying) { false }

        shared_examples_for 'incrementing the busy count on a queue' do |expected_busy|
          it 'should increment the busy count on the queue' do
            subject.allocate(amount)
            expect(global_redis.get('queue:busy:queue')).to eq(expected_busy)
          end
        end

        shared_examples_for 'allocating work from the resource' do
          let(:amount) { 5 }
          let(:limit) { nil }
          let(:workload) { amount.times.map { SecureRandom.uuid } }
          let(:allocated_work) do
            workload.reverse.map do |work|
              [queue, -1, work]
            end.flatten
          end
          let(:queue) { 'queue' }
          let(:list_of_queues) { [queue] }
          let(:shuffled_queues) { list_of_queues }
          let(:queue_group) { :* }

          before do
            allow(subject).to receive(:queues).with(queue_group).and_return(list_of_queues)
            allow(list_of_queues).to receive(:shuffle).and_return(shuffled_queues)

            subject.limit = limit
            workload.each { |work| subject.add_work(queue, work, retrying) }
          end

          it 'should allocate the specified amount from the resource' do
            subject.allocate(amount)
            expect(subject.allocated).to eq(5)
          end

          it 'should return the allocated work (in reverse order) paired with the source queue' do
            expect(subject.allocate(amount)).to eq(allocated_work)
          end

          it 'should remove the work from the queue' do
            subject.allocate(amount)
            expect(global_redis.lrange('resource:queue:queue:resourceful', 0, -1))
          end

          context 'with a different queue group' do
            let(:queue_group) { Faker::Lorem.sentence }

            it 'should fetch work from the filtered queues' do
              expect(subject.allocate(amount, queue_group)).to eq(allocated_work)
            end
          end

          it_behaves_like 'incrementing the busy count on a queue', '5'

          context 'when the resource is frozen' do
            before { subject.frozen = true }

            it 'should not allocate anything' do
              subject.allocate(amount)
              expect(subject.allocated).to eq(0)
            end
          end

          context 'with a different resource' do
            let(:name) { 'resourceless' }

            it 'should return the allocated work' do
              expect(subject.allocate(amount)).to eq(allocated_work)
            end

            it 'should remove the work from the queue' do
              subject.allocate(amount)
              expect(global_redis.lrange('resource:queue:queue:resourceful', 0, -1))
            end

            context 'when the resource is frozen' do
              before { subject.frozen = true }

              it 'should not allocate anything' do
                subject.allocate(amount)
                expect(subject.allocated).to eq(0)
              end
            end
          end

          context 'with a different workload' do
            let(:amount) { 17 }

            it 'should allocate the specified amount from the resource' do
              subject.allocate(amount)
              expect(subject.allocated).to eq(17)
            end

            it_behaves_like 'incrementing the busy count on a queue', '17'
          end

          context 'when the queue does not contain enough work' do
            let(:workload) { 2.times.map { SecureRandom.uuid } }

            it 'should only allocate what is available' do
              subject.allocate(amount)
              expect(subject.allocated).to eq(2)
            end

            it_behaves_like 'incrementing the busy count on a queue', '2'
          end

          context 'when allocated multiple times' do
            let(:workload) { (2 * amount).times.map { SecureRandom.uuid } }

            it 'should allocate the specified amount from the resource' do
              2.times { subject.allocate(amount) }
              expect(subject.allocated).to eq(10)
            end
          end

          context 'with a limit specified' do
            let(:limit) { 5 }

            context 'when allocating more than the limit' do
              let(:allocated_work) do
                workload.reverse[0...limit].map.with_index do |work, allocation|
                  [queue, "#{allocation+1}", work]
                end.flatten
              end
              let(:amount) { 7 }

              it 'should allocate no more than the available amount of resources' do
                subject.allocate(amount)
                expect(subject.allocated).to eq(5)
              end

              it 'should remove allocations from the pool' do
                subject.allocate(amount)
                expect(subject.allocations_left).to eq(0)
              end

              context 'when there are allocations left in the pool' do
                let(:amount) { 3 }

                it 'should allocate no more than the available amount of resources' do
                  subject.allocate(amount)
                  expect(subject.allocated).to eq(3)
                end

                it 'should remove allocations from the pool' do
                  subject.allocate(amount)
                  expect(subject.allocations_left).to eq(2)
                end
              end

              it 'should return the allocated work' do
                expect(subject.allocate(amount)).to match_array(allocated_work)
              end

              it_behaves_like 'incrementing the busy count on a queue', '5'

              context 'with multiple queues having work' do
                let(:queue_two) { 'queue][' }
                let(:list_of_queues) { [queue, queue_two] }
                let(:limit) { 1 }

                before { workload.each { |work| subject.add_work(queue_two, work) } }

                it 'should respect the limit across all queues' do
                  expect(subject.allocate(amount).count).to eq(3)
                end
              end

              context 'when called multiple times' do
                let(:amount) { 3 }
                let(:workload) { limit.times.map { SecureRandom.uuid } }

                it 'should allocate no more than the available amount of resources' do
                  2.times { subject.allocate(amount) }
                  expect(subject.allocated).to eq(5)
                end
              end

              context 'when the limit changes' do
                let(:amount) { 5 }

                it 'should leave the current allocation alone' do
                  subject.allocate(amount)
                  subject.limit = 1
                  subject.allocate(amount)
                  expect(subject.allocated).to eq(5)
                end
              end
            end
          end

          context 'with multiple queues' do
            let(:amount) { 10 }
            let(:workload) { 5.times.map { SecureRandom.uuid } }
            let(:workload_two) { 5.times.map { SecureRandom.uuid } }
            let(:queue_two) { 'queue_two' }
            let(:allocated_work) do
              workload.reverse.map do |work|
                [queue, -1, work]
              end.flatten
            end
            let(:allocated_work_two) do
              workload_two.reverse.map do |work|
                [queue_two, -1, work]
              end.flatten
            end
            let(:list_of_queues) { [queue, queue_two] }

            before do
              workload_two.each { |work| subject.add_work(queue_two, work) }
            end

            it 'should return the allocated work paired with the source queue' do
              expect(subject.allocate(amount)).to match_array(allocated_work + allocated_work_two)
            end

            context 'when a queue is paused' do
              let(:expected_missing_work) do
                workload_two.reverse.map do |work|
                  [queue_two, work]
                end.flatten
              end

              before { Queue.new(queue_two).paused = true }

              it 'should only return work from un-paused queues' do
                expect(subject.allocate(amount)).not_to include(*expected_missing_work)
              end
            end

            context 'when the amount of work available is greater than the work requested' do
              let(:amount) { 5 }

              it 'should return only the allocated work requested' do
                expect(subject.allocate(amount)).to match_array(allocated_work)
              end

              describe 'load balancing' do
                let(:shuffled_queues) { list_of_queues.reverse }

                it 'should shuffle the list of queues' do
                  expect(subject.allocate(amount)).to match_array(allocated_work_two)
                end
              end
            end
          end

          describe 'backing up allocated work' do
            let(:amount) { 1 }
            let(:host) { Faker::Internet.ip_v4_address }
            let(:backup_data) do
              {'queue' => queue, 'resource' => name, 'work' => workload.first}
            end
            let(:result) do
              JSON.load(global_redis.lindex("resource:backup:worker:#{host}", 0))
            end

            before do
              allow_any_instance_of(Resource).to receive(:identity).and_return(host)
              subject.allocate(amount)
            end

            it 'should backup the work to a queue identified by the worker' do
              expect(result).to eq(backup_data)
            end
          end
        end

        it_behaves_like 'allocating work from the resource'

        context 'when the queue is retrying work' do
          let(:retrying) { true }
          it_behaves_like 'allocating work from the resource'
        end

        describe 'retrying work priority' do
          let(:queue) { Faker::Lorem.word }
          let(:work) { SecureRandom.uuid }
          let(:other_work) { SecureRandom.uuid }
          let(:result_work) { subject.allocate(1)[2] }

          before do
            subject.add_work(queue, work, true)
            subject.add_work(queue, other_work, false)
          end

          it 'should prioritize retrying work over regular work' do
            expect(result_work).to eq(work)
          end
        end
      end

      describe '#free' do
        let(:queue) { 'queue' }
        let(:allocated_work) { [] }
        let(:host) { Faker::Internet.ip_v4_address }
        let(:backup_work) do
          global_redis.lrange("resource:backup:worker:#{host}", 0, -1).map do |serialized_work|
            JSON.load(serialized_work)['work']
          end
        end
        let(:work_count) { 5 }
        let(:limit) { nil }

        before do
          subject.limit = limit
          allow_any_instance_of(Resource).to receive(:identity).and_return(host)
          work_count.times { subject.add_work(queue, SecureRandom.uuid) }
          allocated_work.concat subject.allocate(work_count).each_slice(3).map { |_, _, work| work }
        end

        it 'should decrement the allocation count' do
          subject.free(queue, -1, allocated_work.first)
          expect(subject.allocated).to eq(4)
        end

        it 'should decrement the queue busy count' do
          subject.free(queue, -1, allocated_work.first)
          expect(global_redis.get('queue:busy:queue')).to eq('4')
        end

        context 'when the resource has a limit' do
          let(:name) { Faker::Lorem.sentence }
          let(:limit) { work_count }

          it 'should return the allocation to the pool' do
            subject.free(queue, 1, allocated_work.first)
            expect(subject.allocations_left).to eq(1)
          end
        end

        context 'when the work has already been removed' do
          let(:work_count) { 1 }
          before do
            2.times { subject.free(queue, -1, allocated_work.first) }
          end

          it 'should not decrement the allocation count' do
            expect(subject.allocated).to eq(0)
          end

          it 'should not store an over allocation count' do
            expect(global_redis.get("resource:over-allocated:#{name}")).to be_nil
          end

          it 'should only decrement the queue busy once' do
            expect(global_redis.get('queue:busy:queue')).to eq('0')
          end

          context 'when the resource has a limit' do
            let(:name) { Faker::Lorem.sentence }
            let(:limit) { work_count }

            it 'should leave the pool alone' do
              subject.free(queue, 1, allocated_work.first)
              expect(subject.allocations_left).to eq(0)
            end
          end
        end

        context 'when more work has been done than is available from this resource' do
          before { global_redis.set("resource:allocated:#{name}", '0') }

          it 'should keep the allocation count at 0' do
            subject.free(queue, -1, allocated_work.first)
            expect(subject.allocated).to eq(0)
          end

          it 'should store a value indicating that the resource had been over-allocated' do
            subject.free(queue, -1, allocated_work.first)
            expect(global_redis.get("resource:over-allocated:#{name}")).to eq('1')
          end

          it 'should increment the queue busy by the negative difference' do
            subject.free(queue, -1, allocated_work.first)
            expect(global_redis.get('queue:busy:queue')).to eq('5')
          end
        end

        it 'should remove the work from the backup queue' do
          subject.free(queue, -1, allocated_work.first)
          expect(backup_work).not_to include(allocated_work.first)
        end

        context 'with a different resource' do
          let(:name) { 'heavy_workload' }
          let(:queue) { 'busy_queue' }

          it 'should decrement the allocation count' do
            subject.free(queue, -1, allocated_work.first)
            expect(subject.allocated).to eq(4)
          end

          it 'should decrement the queue busy count' do
            subject.free(queue, -1, allocated_work.first)
            expect(global_redis.get('queue:busy:busy_queue')).to eq('4')
          end
        end
      end

    end
  end
end
