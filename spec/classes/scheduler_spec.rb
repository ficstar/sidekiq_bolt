require 'rspec'

module Sidekiq
  module Bolt
    describe Scheduler do

      let(:klass) { Scheduler }
      let(:worker_class_base) do
        Class.new do
          include Worker

          def self.to_s
            @name ||= Faker::Lorem.word
          end
        end
      end
      let(:serialized_work) { global_redis.lrange("successive_work:#{job_id}", 0, -1).first }
      let(:result_item) { JSON.load(serialized_work) }
      let(:result_work) { Sidekiq.load_json(result_item['work']) if result_item }
      let(:serialized_work_two) { global_redis.lrange("successive_work:#{new_jid}", 0, -1).first }

      it_behaves_like 'a job scheduler'

      describe '#perform_after_with_options' do
        let(:job_id) { SecureRandom.uuid }
        let(:scheduler) { Scheduler.new('jid' => job_id) }
        let(:result_item) { JSON.load(serialized_work) }
        let(:result_queue) { result_item['queue'] }
        let(:result_resource) { result_item['resource'] }
        let(:result_work) { Sidekiq.load_json(result_item['work']) if result_item }

        describe 'custom parent job id' do
          let(:custom_jid) { SecureRandom.uuid }
          let(:options) { {parent_job_id: custom_jid} }

          before do
            scheduler.perform_after_with_options(options, worker_class_base)
            scheduler.schedule!
          end

          it 'should use that job id' do
            expect(result_work['pjid']).to eq(custom_jid)
          end

          describe 'persisting results to redis' do
            let(:options) { {persist_result: true} }

            it 'should indicate that this result will be persisted so that it may be picked up later' do
              expect(result_work['persist']).to eq(true)
            end

            context 'when not to be persisted' do
              let(:options) { {} }

              it 'should not include that key' do
                expect(result_work).not_to include('persist')
              end
            end
          end
        end
      end

    end
  end
end
