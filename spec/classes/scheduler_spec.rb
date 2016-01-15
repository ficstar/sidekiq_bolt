require 'rspec'

module Sidekiq
  module Bolt
    describe Scheduler do

      let(:worker_class_name) { worker_class.to_s }
      let(:worker_class_base) do
        Class.new do
          include Worker

          def self.to_s
            @name ||= Faker::Lorem.word
          end
        end
      end
      let(:worker_class) { worker_class_base }
      let(:job_id) { SecureRandom.uuid }
      let(:parent) { {'jid' => job_id} }
      let(:scheduler) { Scheduler.new(parent) }
      let(:args) { Faker::Lorem.paragraphs }

      subject { scheduler }

      describe '#perform_after' do
        let(:serialized_work) { global_redis.lrange("successive_work:#{job_id}", 0, -1).first }
        let(:result_item) { JSON.load(serialized_work) }
        let(:result_queue) { result_item['queue'] }
        let(:result_resource) { result_item['resource'] }
        let(:result_work) { Sidekiq.load_json(result_item['work']) if result_item }
        let(:new_jid) { SecureRandom.uuid }
        #noinspection RubyStringKeysInHashInspection
        let(:expected_work) do
          {
              'class' => worker_class_name,
              'jid' => new_jid,
              'queue' => 'default',
              'resource' => 'default',
              'args' => args,
              'retry' => true
          }
        end

        before do
          allow(SecureRandom).to receive(:base64).with(16).and_return(new_jid)
          scheduler.perform_after(worker_class, *args)
          scheduler.schedule!
        end

        it 'should schedule this work to run after the previous job' do
          expect(result_work).to eq(expected_work)
        end

        it 'should schedule to run in the default queue' do
          expect(result_queue).to eq('default')
        end

        it 'should schedule to run in the default resource' do
          expect(result_resource).to eq('default')
        end

        context 'when the worker overrides the queue' do
          let(:worker_class) { Class.new(worker_class_base) { sidekiq_options queue: 'other queue' } }
          it { expect(result_queue).to eq('other queue') }
        end

        context 'when the worker overrides the resource' do
          let(:worker_class) { Class.new(worker_class_base) { sidekiq_options resource: 'other resource' } }
          it { expect(result_resource).to eq('other resource') }
        end

        context 'when the worker overrides the retry behaviour' do
          let(:worker_class) { Class.new(worker_class_base) { sidekiq_options retry: false } }
          it { expect(!!result_work['retry']).to eq(false) }
        end

      end

    end
  end
end
