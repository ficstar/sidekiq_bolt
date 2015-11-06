require 'rspec'

module Sidekiq
  module Bolt
    describe Resource do

      let(:name) { 'resourceful' }

      subject { Resource.new(name) }

      describe '#all' do
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
        let(:limit) { 5 }

        before { subject.limit = limit }

        it 'should be the specified limit' do
          expect(subject.limit).to eq(5)
        end

        it 'should store the value in redis' do
          expect(global_redis.get('resource:limit:resourceful')).to eq('5')
        end

        it 'should be an integer property' do
          expect(Resource.new(name).limit).to eq(5)
        end
      end

      describe '#allocated' do
        let(:busy) { 5 }

        before { global_redis.set("resource:allocated:#{name}", busy) }

        its(:allocated) { is_expected.to eq(5) }

        context 'with a different resource' do
          let(:busy) { 15 }
          let(:name) { 'really_busy' }

          its(:allocated) { is_expected.to eq(15) }
        end
      end

      describe '#add_work' do
        let(:work) { 'piece_of_work' }
        let(:queue) { 'workload' }

        it 'should add the new work to the specified queue' do
          subject.add_work(queue, work)
          expect(global_redis.rpop('resource:queue:workload:resourceful')).to eq('piece_of_work')
        end

        it 'should associate the queue with the resource' do
          subject.add_work(queue, work)
          expect(global_redis.smembers('resource:queues:resourceful')).to include('workload')
        end

        it 'should store the resource in the global list' do
          subject.add_work(queue, work)
          expect(global_redis.smembers('resources')).to include('resourceful')
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

          it 'should store the resource in the global list' do
            subject.add_work(queue, work)
            expect(global_redis.smembers('resources')).to include('heavy_duty')
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
        let(:queues) { %w(queue1 queue2) }

        before do
          queues.each { |queue| subject.add_work(queue, SecureRandom.uuid) }
        end

        it 'should return the list of queue names associated with this resource' do
          expect(subject.queues).to match_array(queues)
        end

        context 'with a different resource' do
          let(:name) { 'heavy_worker' }
          let(:queues) { %w(big_queue1 big_queue2) }

          it 'should return the list of queue names associated with this resource' do
            expect(subject.queues).to match_array(queues)
          end
        end
      end

      describe '#allocate' do
        let(:amount) { 5 }
        let(:limit) { nil }
        let(:workload) { amount.times.map { SecureRandom.uuid } }
        let(:allocated_work) do
          workload.reverse.map do |work|
            [queue, work]
          end.flatten
        end
        let(:queue) { 'queue' }
        let(:list_of_queues) { [queue] }
        let(:shuffled_queues) { list_of_queues }

        before do
          allow(subject).to receive(:queues).and_return(list_of_queues)
          allow(list_of_queues).to receive(:shuffle).and_return(shuffled_queues)

          subject.limit = limit
          workload.each { |work| subject.add_work(queue, work) }
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

        it 'should increment the busy count on the queue' do
          subject.allocate(amount)
          expect(global_redis.get('queue:busy:queue')).to eq('5')
        end

        context 'with a different workload' do
          let(:amount) { 17 }

          it 'should allocate the specified amount from the resource' do
            subject.allocate(amount)
            expect(subject.allocated).to eq(17)
          end

          it 'should increment the busy count on the queue' do
            subject.allocate(amount)
            expect(global_redis.get('queue:busy:queue')).to eq('17')
          end
        end

        context 'when the queue does not contain enough work' do
          let(:workload) { 2.times.map { SecureRandom.uuid } }

          it 'should only allocate what is available' do
            subject.allocate(amount)
            expect(subject.allocated).to eq(2)
          end

          it 'should increment the busy count on the queue by the amount of actual work' do
            subject.allocate(amount)
            expect(global_redis.get('queue:busy:queue')).to eq('2')
          end
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
              workload.reverse[0...limit].map do |work|
                [queue, work]
              end.flatten
            end
            let(:amount) { 7 }

            it 'should allocate no more than the available amount of resources' do
              subject.allocate(amount)
              expect(subject.allocated).to eq(5)
            end

            it 'should return the allocated work' do
              expect(subject.allocate(amount)).to match_array(allocated_work)
            end

            it 'should increment the busy count on the queue by the amount of actual work' do
              subject.allocate(amount)
              expect(global_redis.get('queue:busy:queue')).to eq('5')
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
              [queue, work]
            end.flatten
          end
          let(:allocated_work_two) do
            workload_two.reverse.map do |work|
              [queue_two, work]
            end.flatten
          end
          let(:list_of_queues) { [queue, queue_two] }

          before do
            workload_two.each { |work| subject.add_work(queue_two, work) }
          end

          it 'should return the allocated work paired with the source queue' do
            expect(subject.allocate(amount)).to match_array(allocated_work + allocated_work_two)
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
      end

      describe '#free' do
        let(:queue) { 'queue' }

        before do
          5.times { subject.add_work(queue, SecureRandom.uuid) }
          global_redis.incrby("resource:allocated:#{name}", 5)
          global_redis.incrby("queue:busy:#{queue}", 5)
        end

        it 'should decrement the allocation count' do
          subject.free(queue)
          expect(subject.allocated).to eq(4)
        end

        it 'should decrement the queue busy count' do
          subject.free(queue)
          expect(global_redis.get('queue:busy:queue')).to eq('4')
        end

        context 'with a different resource' do
          let(:name) { 'heavy_workload' }
          let(:queue) { 'busy_queue' }

          it 'should decrement the allocation count' do
            subject.free(queue)
            expect(subject.allocated).to eq(4)
          end

          it 'should decrement the queue busy count' do
            subject.free(queue)
            expect(global_redis.get('queue:busy:busy_queue')).to eq('4')
          end
        end
      end

    end
  end
end
