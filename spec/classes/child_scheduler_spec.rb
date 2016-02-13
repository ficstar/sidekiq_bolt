require 'rspec'

module Sidekiq
  module Bolt
    describe ChildScheduler do

      let(:klass) { ChildScheduler }
      let(:worker_class_base) do
        Class.new do
          include Worker

          def self.to_s
            @name ||= Faker::Lorem.word
          end
        end
      end
      let(:job_id) { SecureRandom.uuid }
      let(:scheduler) { ChildScheduler.new('jid' => job_id) }
      let(:resource_name) { 'default' }
      let(:resource) { Resource.new(resource_name) }
      let(:result_allocation) { resource.allocate(1) }
      let(:serialized_work) { result_allocation[1] }
      let(:serialized_work_two) { global_redis.lrange("successive_work:#{new_jid}", 0, -1).first }
      let(:result_item) { Sidekiq.load_json(serialized_work) if serialized_work }
      let(:result_queue) { result_item['queue'] }
      let(:result_resource) { result_item['resource'] }
      let(:result_work) { result_item }

      it_behaves_like 'a job scheduler'

      describe '#perform_after_with_options' do
        let(:options) { {} }

        before { scheduler.perform_after_with_options(options, worker_class_base) }

        describe 'custom parent job id' do
          let(:custom_jid) { SecureRandom.uuid }
          let(:options) { {parent_job_id: custom_jid} }

          before { scheduler.schedule! }

          it 'should use the scheduler job id' do
            expect(result_work['pjid']).to eq(job_id)
          end
        end

        context 'when the number of scheduled items exceeds a configured buffer size' do
          let(:buffer_size) { 1 }
          let(:sidekiq_options) { {child_scheduler_buffer_size: buffer_size} }

          it 'should immediately schedule the work' do
            expect(result_work).not_to be_nil
          end

          it 'should not schedule the same work twice' do
            resource.allocate(1)
            scheduler.schedule!
            expect(resource.allocate(1)).to be_empty
          end

          context 'when there is some space left' do
            let(:buffer_size) { 3 }

            it 'should not schedule the work' do
              expect(result_work).to be_nil
            end
          end
        end
      end

    end
  end
end