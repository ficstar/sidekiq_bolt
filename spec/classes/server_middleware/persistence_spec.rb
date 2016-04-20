require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe Persistence do

        describe '#call' do
          let(:parent_job_id) { SecureRandom.uuid }
          let(:job_id) { SecureRandom.uuid }
          let(:resource_name) { Faker::Lorem.word }
          let(:persist_result) { true }
          #noinspection RubyStringKeysInHashInspection
          let(:job) { {'pjid' => parent_job_id, 'jid' => job_id, 'resource' => resource_name, 'persist' => persist_result} }
          let(:next_job) { {} }
          let(:expected_result) { Faker::Lorem.word }
          let(:serialized_result) { global_redis.get("worker:results:#{job_id}") }
          let(:block_result) { expected_result }
          let(:result) { Sidekiq.load_json(serialized_result) if serialized_result }
          let(:block) { ->() { block_result } }
          let(:worker) { nil }

          it_behaves_like 'a server middleware'

          it 'should save the result to redis' do
            subject.call(nil, job, nil, &block).get
            expect(result).to eq(expected_result)
          end

          context 'when the work is not to be persisted' do
            let(:persist_result) { nil }

            it 'should not save the result to redis when complete' do
              subject.call(nil, job, nil, &block).get
              expect(result).to be_nil
            end
          end

          context 'when the job id has been removed' do
            let(:serialized_result) { global_redis.get('worker:results:') }

            before { job.delete('jid') }

            it 'should not save the result to redis' do
              subject.call(nil, job, nil, &block).get
              expect(result).to be_nil
            end
          end

          context 'with an error' do
            let(:error) { StandardError.new(Faker::Lorem.sentence) }
            let(:block_result) { ThomasUtils::Future.error(error) }

            describe 'the resulting error' do
              subject { result }

              before { Persistence.new.call(nil, job, nil, &block).get rescue nil }

              it { is_expected.to be_a_kind_of(SerializableError) }
              its(:error_class) { is_expected.to eq(StandardError) }
              its(:message) { is_expected.to eq(error.message) }
            end
          end

        end

      end
    end
  end
end
