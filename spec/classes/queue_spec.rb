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
        let(:allocated_work) { Resource.new(resource_name).allocate(1) }

        before { subject.enqueue(resource_name, workload) }

        it 'should add work to the specified resource' do
          expect(allocated_work).to eq([name, workload])
        end
      end

    end
  end
end