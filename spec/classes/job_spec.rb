require 'rspec'

module Sidekiq
  module Bolt
    describe Job do

      let(:name) { Faker::Lorem.sentence }

      subject { Job.new(name) }

      describe '#name' do
        its(:name) { is_expected.to eq(name) }
      end

      describe '#add_queue' do
        let(:queue_name) { SecureRandom.uuid }

        it 'adds the queue to this job' do
          subject.add_queue(queue_name)
          expect(global_redis.smembers("job:queues:#{name}")).to include(queue_name)
        end
      end

      describe '#queues' do
        let(:queue_name) { SecureRandom.uuid }

        before { subject.add_queue(queue_name) }

        its(:queues) { is_expected.to include(Queue.new(queue_name)) }

        context 'with mutliple queues' do
          let(:queue_name_two) { SecureRandom.uuid }

          before { subject.add_queue(queue_name_two) }

          its(:queues) { is_expected.to match_array([Queue.new(queue_name), Queue.new(queue_name_two)]) }
        end
      end

    end
  end
end
