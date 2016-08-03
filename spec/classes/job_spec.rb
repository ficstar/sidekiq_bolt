require 'rspec'

module Sidekiq
  module Bolt
    describe Job do

      let(:name) { Faker::Lorem.sentence }

      subject { Job.new(name) }

      describe '.all' do
        let(:job_names) { Faker::Lorem.words }
        let(:expected_jobs) { job_names.map { |name| Job.new(name) } }

        subject { Job.all }

        before { expected_jobs.each { |job| job.add_queue(SecureRandom.uuid) } }

        it { is_expected.to match_array(expected_jobs) }
      end

      describe '#name' do
        its(:name) { is_expected.to eq(name) }
      end

      describe '#add_queue' do
        let(:queue_name) { SecureRandom.uuid }

        it 'adds the queue to this job' do
          subject.add_queue(queue_name)
          expect(global_redis.smembers("job:queues:#{name}")).to include(queue_name)
        end

        it 'creates a reference from the queue to the job' do
          subject.add_queue(queue_name)
          expect(global_redis.get("queue:job:#{queue_name}")).to eq(name)
        end

        it 'creates a reference indicating that this job exists' do
          subject.add_queue(queue_name)
          expect(global_redis.smembers('jobs')).to include(name)
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
