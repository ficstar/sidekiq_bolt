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
          let(:resource) { Resource.new(Faker::Lorem.word) }
          let(:resource_attribute_count) { resource.public_send(method) }

          before do
            work_count.times { resource.add_work(name, SecureRandom.uuid, retrying) }
          end

          its(method) { is_expected.to eq(resource_attribute_count) }

          context 'with multiple resources' do
            let(:work_count_two) { rand(1..10) }
            let(:resource_two) { Resource.new(Faker::Lorem.word) }
            let(:resource_attribute_count_two) { resource_two.public_send(method) }

            before do
              work_count_two.times { resource_two.add_work(name, SecureRandom.uuid) }
            end

            its(method) { is_expected.to eq(resource_attribute_count + resource_attribute_count_two) }
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
      end

      describe '#enqueue' do
        let(:resource_name) { Faker::Lorem.word }
        let(:workload) { SecureRandom.uuid }
        let(:resource) { Resource.new(resource_name) }
        let(:allocated_work) { resource.allocate(1) }

        context 'when the work is not retrying' do
          before { subject.enqueue(resource_name, workload) }

          it 'should add work to the specified resource' do
            expect(allocated_work).to eq([name, workload])
          end

          it 'should not add this work to the retrying queue' do
            expect(resource.retrying).to eq(0)
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
