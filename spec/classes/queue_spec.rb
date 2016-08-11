require 'rspec'

module Sidekiq
  module Bolt
    describe Queue do

      let(:name) { Faker::Lorem.word }
      let(:queue) { Queue.new(name) }

      subject { queue }

      describe '.all' do
        let(:resource) { Resource.new(Faker::Lorem.word) }

        subject { Queue.all }

        before { resource.add_work(name, '') }

        it { is_expected.to eq([queue]) }

        context 'with multiple queues' do
          let(:resource_two) { Resource.new(Faker::Lorem.word) }
          let(:name_two) { Faker::Lorem.word }
          let(:queue_two) { Queue.new(name_two) }

          before { resource_two.add_work(name_two, '') }

          it { is_expected.to match_array([queue, queue_two]) }
        end
      end

      describe '.enqueue' do
        let(:queue) { SecureRandom.uuid }
        let(:resource) { SecureRandom.uuid }
        let(:work) { SecureRandom.uuid }
        let(:item) { {queue: queue, resource: resource, work: work} }
        let(:items) { [item] }
        let(:result_allocation) { Resource.new(resource).allocate(1) }
        let(:result_queue) { result_allocation[0] }
        let(:result_work) { result_allocation[1] }

        before { Queue.enqueue(items) }

        it 'should enqueue the work' do
          expect(result_work).to eq(work)
        end

        it 'should enqueue the work to the specified queue' do
          expect(result_queue).to eq(queue)
        end

        context 'with multiple items' do
          let(:queue_two) { SecureRandom.uuid }
          let(:resource_two) { SecureRandom.uuid }
          let(:work_two) { SecureRandom.uuid }
          let(:item_two) { {queue: queue_two, resource: resource_two, work: work_two} }
          let(:items) { [item, item_two] }
          let(:result_allocation_two) { Resource.new(resource_two).allocate(1) }
          let(:result_queue_two) { result_allocation_two[0] }
          let(:result_work_two) { result_allocation_two[1] }

          it 'should enqueue the work' do
            expect(result_work_two).to eq(work_two)
          end

          it 'should enqueue the work to the specified queue' do
            expect(result_queue_two).to eq(queue_two)
          end
        end
      end

      describe '#name' do
        its(:name) { is_expected.to eq(name) }
      end

      describe '#resources' do
        let(:resource) { Resource.new(Faker::Lorem.word) }

        before { resource.add_work(name, '') }

        its(:resources) { is_expected.to eq([resource]) }

        context 'with multiple resources' do
          let(:resource_two) { Resource.new(Faker::Lorem.word) }
          let(:name_two) { name }

          before { resource_two.add_work(name_two, '') }

          its(:resources) { is_expected.to match_array([resource, resource_two]) }

          context 'when the resource does not associate with this queue' do
            let(:name_two) { Faker::Lorem.word }

            its(:resources) { is_expected.to eq([resource]) }
          end
        end
      end

      describe '#job' do
        its(:job) { is_expected.to be_nil }

        context 'with an associated job' do
          let(:job_name) { Faker::Lorem.sentence }
          let(:job) { Job.new(job_name) }

          before { job.add_queue(name) }

          its(:job) { is_expected.to eq(job) }
        end
      end

      shared_examples_for 'a queue attribute preventing operation on the queue' do |attribute|
        let(attribute) { false }

        before do
          subject.public_send(:"#{attribute}=", true)
          subject.public_send(:"#{attribute}=", send(attribute))
        end

        it 'should store the value in redis' do
          expect(!!global_redis.get("queue:#{attribute}:#{name}")).to eq(false)
        end

        its(attribute) { is_expected.to eq(false) }

        context 'when paused' do
          let(attribute) { true }

          it 'should store the value in redis' do
            expect(!!global_redis.get("queue:#{attribute}:#{name}")).to eq(true)
          end

          its(attribute) { is_expected.to eq(true) }
        end
      end

      describe '#paused' do
        it_behaves_like 'a queue attribute preventing operation on the queue', :paused
      end

      describe '#blocked' do
        it_behaves_like 'a queue attribute preventing operation on the queue', :blocked
      end

      shared_examples_for 'counting attributes for queue resources' do |method, retrying|
        its(method) { is_expected.to eq(0) }

        context 'with resources' do
          let(:work_count) { rand(1..10) }
          let(:resource) { Resource.new(Faker::Lorem.sentence) }
          let(:resource_attribute_count) { work_count }

          before do
            work_count.times { resource.add_work(name, SecureRandom.uuid, retrying) }
          end

          its(method) { is_expected.to eq(resource_attribute_count) }

          context 'with multiple resources' do
            let(:work_count_two) { rand(1..10) }
            let(:resource_two) { Resource.new(Faker::Lorem.sentence) }
            let(:resource_attribute_count_two) { work_count_two }

            before do
              work_count_two.times { resource_two.add_work(name, SecureRandom.uuid, retrying) }
            end

            its(method) { is_expected.to eq(resource_attribute_count + resource_attribute_count_two) }
          end

          context 'with multiple queues sharing the resource' do
            let(:queue_two) { Faker::Lorem.sentence }
            let(:work_count_two) { rand(1..10) }

            before do
              work_count_two.times { resource.add_work(queue_two, SecureRandom.uuid) }
            end

            its(method) { is_expected.to eq(resource_attribute_count) }
          end
        end
      end

      describe '#size' do
        it_behaves_like 'counting attributes for queue resources', :size, false
      end

      describe '#retrying' do
        it_behaves_like 'counting attributes for queue resources', :retrying, true

        context 'when the retry comes from the retry set' do
          let(:resource_name) { Faker::Lorem.word }
          let(:serialized_msg) do
            JSON.dump('queue' => name, 'resource' => resource_name)
          end

          before do
            global_redis.zadd('bolt:retry', Time.now.to_f, serialized_msg)
          end

          its(:retrying) { is_expected.to eq(1) }

          context 'with multiple retrying items' do
            let(:resource_name_two) { Faker::Lorem.word }
            let(:serialized_msg_two) do
              JSON.dump('queue' => name, 'resource' => resource_name_two)
            end

            before do
              global_redis.zadd('bolt:retry', Time.now.to_f, serialized_msg_two)
            end

            its(:retrying) { is_expected.to eq(2) }
          end

          context 'with a different queue' do
            let(:queue_two) { Faker::Lorem.word }
            let(:serialized_msg_two) do
              JSON.dump('queue' => queue_two, 'resource' => resource_name)
            end

            before do
              global_redis.zadd('bolt:retry', Time.now.to_f, serialized_msg_two)
            end

            its(:retrying) { is_expected.to eq(1) }
          end
        end
      end

      describe '#error_count' do
        its(:error_count) { is_expected.to eq(0) }

        context 'with errors' do
          let(:error_count) { rand(1..100) }

          before { global_redis.hincrby("queue:stats:#{name}", 'error', error_count) }

          its(:error_count) { is_expected.to eq(error_count) }
        end
      end

      describe '#busy' do
        its(:busy) { is_expected.to eq(0) }

        context 'with resources' do
          let(:resource) { Resource.new(Faker::Lorem.word) }

          before do
            rand(1..10).times { resource.add_work(name, SecureRandom.uuid) }
            resource.allocate(10)
          end

          its(:busy) { is_expected.to eq(resource.allocated) }

          context 'with multiple resources' do
            let(:resource_two) { Resource.new(Faker::Lorem.word) }

            before do
              rand(1..10).times { resource_two.add_work(name, SecureRandom.uuid) }
              resource_two.allocate(10)
            end

            its(:busy) { is_expected.to eq(resource.allocated + resource_two.allocated) }
          end
        end

        context 'with multiple queues sharing the same resource' do
          let(:resource) { Resource.new(Faker::Lorem.word) }
          let(:name_two) { Faker::Lorem.sentence }
          let(:queue_two) { Queue.new(name_two) }

          before do
            3.times { resource.add_work(name, SecureRandom.uuid) }
            2.times { resource.add_work(name_two, SecureRandom.uuid) }
            resource.allocate(5)
          end

          it 'should isolate the first queues from the second' do
            expect(subject.busy).to eq(3)
          end

          it 'should isolate the second queues from the first' do
            expect(queue_two.busy).to eq(2)
          end
        end
      end

      describe '#enqueue' do
        let(:resource_name) { Faker::Lorem.word }
        let(:resource_param) { resource_name }
        let(:workload) { SecureRandom.uuid }
        let(:resource) { Resource.new(resource_name) }
        let(:allocated_work) { resource.allocate(1) }

        context 'when the work is not retrying' do
          before { subject.enqueue(resource_param, workload) }

          it 'should add work to the specified resource' do
            expect(allocated_work).to eq([name, workload])
          end

          it 'should not add this work to the retrying queue' do
            expect(resource.retrying).to eq(0)
          end

          context 'when the resource is an object, rather than a name' do
            let(:resource_param) { resource }
            let(:resource) { Resource.new(resource_name) }

            it 'should add work to the specified resource' do
              expect(allocated_work).to eq([name, workload])
            end
          end
        end

        context 'when the work is retrying' do
          before { subject.enqueue(resource_name, workload, true) }

          it 'should add this work to the retrying queue' do
            expect(resource.retrying).to eq(1)
          end
        end
      end

    end
  end
end
