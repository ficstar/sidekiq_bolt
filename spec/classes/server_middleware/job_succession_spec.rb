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
        let(:dependencies) { [job_id, SecureRandom.uuid] }

        before do
          dependencies.each { |jid| global_redis.sadd("dependencies:#{parent_job_id}", jid) }
          global_redis.set("parent:#{job_id}", parent_job_id)
        end

        it 'should yield' do
          expect { |block| subject.call(nil, job, nil, &block) }.to yield_control
        end

        shared_examples_for 'removing job dependencies' do
          let(:grandparent_job_id) { SecureRandom.uuid }

          before do
            global_redis.sadd("dependencies:#{grandparent_job_id}", parent_job_id)
            global_redis.set("parent:#{parent_job_id}", grandparent_job_id)
          end

          it 'should remove the parent job dependency' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.smembers("dependencies:#{parent_job_id}")).not_to include(job_id)
          end

          it 'should delete the parent key' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.get("parent:#{job_id}")).to be_nil
          end

          it 'should not remove the grand-parent job dependency' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.smembers("dependencies:#{grandparent_job_id}")).to include(parent_job_id)
          end

          it 'should not delete the grand-parent key' do
            subject.call(nil, job, nil, &block) rescue nil
            expect(global_redis.get("parent:#{parent_job_id}")).to eq(grandparent_job_id)
          end

          context 'when the parent job no longer has any dependencies' do
            let(:dependencies) { [job_id] }

            it 'should remove the grand-parent job dependency' do
              subject.call(nil, job, nil, &block) rescue nil
              expect(global_redis.smembers("dependencies:#{grandparent_job_id}")).not_to include(parent_job_id)
            end

            it 'should delete the grand-parent key' do
              subject.call(nil, job, nil, &block) rescue nil
              expect(global_redis.get("parent:#{parent_job_id}")).to be_nil
            end
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
