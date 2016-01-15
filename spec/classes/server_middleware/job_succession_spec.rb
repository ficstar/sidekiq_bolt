require 'rspec'

module Sidekiq
  module Bolt
    describe JobSuccession do

      describe '#call' do
        let(:parent_job_id) { SecureRandom.uuid }
        let(:job_id) { SecureRandom.uuid }
        #noinspection RubyStringKeysInHashInspection
        let(:job) { {'pjid' => parent_job_id, 'jid' => job_id} }
        let(:next_job) { {} }
        let(:block) { -> {} }

        before do
          global_redis.sadd("dependencies:#{parent_job_id}", job_id)
          global_redis.set("parent:#{job_id}", parent_job_id)
        end

        it 'should yield' do
          expect { |block| subject.call(nil, job, nil, &block) }.to yield_control
        end

        shared_examples_for 'removing job dependencies' do
          before do
            subject.call(nil, job, nil, &block) rescue nil
          end

          it 'should remove the parent job dependency' do
            expect(global_redis.smembers("dependencies:#{parent_job_id}")).to be_empty
          end

          it 'should delete the parent key' do
            expect(global_redis.get("parent:#{job_id}")).to be_nil
          end
        end

        it_behaves_like 'removing job dependencies'

        context 'when the provided block raises an error' do
          let(:block) { ->() { raise 'It broke!' } }

          it_behaves_like 'removing job dependencies'

          it 'should re-raise the error' do
            expect { subject.call(nil, job, nil, &block) }.to raise_error
          end
        end

      end

    end
  end
end
