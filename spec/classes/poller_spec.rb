require 'rspec'

module Sidekiq
  module Bolt
    describe Poller do

      let(:now) { Time.now }

      around { |example| Timecop.freeze(now) { example.run } }

      describe '#enqueue_jobs' do
        let(:queue_name) { Faker::Lorem.word }
        let(:resource_name) { Faker::Lorem.word }
        let(:queue) { Queue.new(queue_name) }
        let(:resource) { Resource.new(resource_name) }
        let(:work_time) { now - 120 }
        let(:work_data) { SecureRandom.uuid }
        #noinspection RubyStringKeysInHashInspection
        let(:work) { {'queue' => queue_name, 'resource' => resource_name, 'work' => work_data} }
        let(:serialized_work) { JSON.dump(work) }
        let(:result_allocation) { resource.allocate(1) }
        let(:result_queue) { result_allocation[0] }
        let(:result_work) { result_allocation[1] }

        it 'should run the default Enq for Sidekiq' do
          expect_any_instance_of(Scheduled::Enq).to receive(:enqueue_jobs)
          Poller.new.enqueue_jobs
        end

        shared_examples_for 'enqueuing scheduled jobs' do |set|
          let(:scheduled_items) { {serialized_work => work_time} }

          before do
            scheduled_items.each { |item, time| global_redis.zadd("bolt:#{set}", time.to_f, item) }
            Poller.new.enqueue_jobs
          end

          it 'should add the work to the queue that it came from' do
            expect(result_queue).to eq(queue_name)
          end

          it 'should keep a reference of the queue name' do
            expect(Queue.all.map(&:name)).to include(queue_name)
          end

          it 'should keep a reference of the resource name' do
            expect(Queue.all.map(&:name)).to include(queue_name)
          end

          it 'should create a reference between the queue and the resource' do
            expect(queue.resources.map(&:name)).to include(resource_name)
          end

          it 'should add the work to the resource that it came from' do
            expect(result_work).to eq(work_data)
          end

          it 'should remove the item from the set' do
            expect(global_redis.zrange("bolt:#{set}", 0, -1)).to be_empty
          end

          context 'when the work is scheduled for later' do
            let(:work_time) { now + 120 }

            it 'should not re-enqueue the work' do
              expect(result_work).to be_nil
            end
          end

          context 'with multiple expired items' do
            let(:work_time_two) { now - 100 }
            let(:result_allocation) { resource.allocate(2) }
            let(:result_work_two) { result_allocation[3] }
            let(:work_data_two) { SecureRandom.uuid }
            #noinspection RubyStringKeysInHashInspection
            let(:work_two) { {'queue' => queue_name, 'resource' => resource_name, 'work' => work_data_two} }
            let(:serialized_work_two) { JSON.dump(work_two) }
            let(:scheduled_items) { {serialized_work => work_time, serialized_work_two => work_time_two} }

            it 'should schedule the first item' do
              expect([result_work, result_work_two]).to include(work_data)
            end

            it 'should schedule the second item' do
              expect([result_work, result_work_two]).to include(work_data_two)
            end
          end
        end

        describe 'enqueueing retries' do
          it_behaves_like 'enqueuing scheduled jobs', :retry

          it 'should count the work as retrying' do
            global_redis.zadd('bolt:retry', work_time.to_f, serialized_work)
            Poller.new.enqueue_jobs
            expect(resource.retrying).to eq(1)
          end
        end

        describe 'enqueueing scheduled jobs' do
          it_behaves_like 'enqueuing scheduled jobs', :scheduled

          it 'should not count the work as retrying' do
            global_redis.zadd('bolt:scheduled', work_time.to_f, serialized_work)
            Poller.new.enqueue_jobs
            expect(resource.retrying).to eq(0)
          end
        end

      end

    end
  end
end
