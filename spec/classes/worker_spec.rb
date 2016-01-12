require 'rspec'

module Sidekiq
  module Bolt
    describe Worker do

      class MockWorker
        include Worker
      end

      class MockWorkerTwo
        include Worker
        RESOURCE_NAME = Faker::Lorem.word
        sidekiq_options resource: RESOURCE_NAME
      end

      class MockWorkerThree
        include Worker
        QUEUE_NAME = Faker::Lorem.word
        sidekiq_options queue: QUEUE_NAME
      end

      let(:args) { Faker::Lorem.paragraphs }
      let(:queue_name) { 'default' }
      let(:resource_name) { 'default' }
      let(:resource) { Resource.new(resource_name) }
      let(:result_work) { resource.allocate(1) }
      let(:result_item) { Sidekiq.load_json(result_work[1]) }
      let(:klass) { MockWorker }

      subject { klass.new }

      it { is_expected.to be_a_kind_of(Sidekiq::Worker) }

      describe '#queue' do
        let(:queue_name) { Faker::Lorem }
        let(:queue) { Queue.new(queue_name) }
        before { subject.queue = queue }
        its(:queue) { is_expected.to eq(queue) }
      end

      describe '#resource' do
        let(:resource_name) { Faker::Lorem }
        let(:resource) { Resource.new(resource_name) }
        before { subject.resource = resource }
        its(:resource) { is_expected.to eq(resource) }
      end

      describe '.sidekiq_should_retry?' do
        let(:some_value) { Faker::Lorem.word }
        let(:retry_block) { ->() { some_value } }
        let(:klass) do
          block = retry_block
          Class.new do
            include Worker
            sidekiq_should_retry?(&block)
          end
        end

        it 'should set the sidekiq_should_retry_block for this worker' do
          expect(subject.sidekiq_should_retry_block.call).to eq(some_value)
        end
      end

      describe '.perform_async' do
        before { klass.perform_async(*args) }

        it 'should enqueue the work to the default queue/resource' do
          expect(result_item).to include('queue' => queue_name, 'resource' => resource_name, 'class' => 'Sidekiq::Bolt::MockWorker', 'args' => args)
        end

        context 'with an overridden resource name' do
          let(:klass) { MockWorkerTwo }
          let(:resource_name) { MockWorkerTwo::RESOURCE_NAME }

          it 'should enqueue the work to the specified resource' do
            expect(result_item).to include('queue' => queue_name, 'resource' => resource_name, 'class' => 'Sidekiq::Bolt::MockWorkerTwo', 'args' => args)
          end
        end
      end

      describe '#perform_in' do
        let(:now) { Time.now }
        let(:interval) { rand(1..999) }
        let(:scheduled_job) { global_redis.zrange('bolt:scheduled', 0, -1).first }
        let(:result_work) { JSON.load(scheduled_job) }
        let(:result_item) { Sidekiq.load_json(result_work['work']) }
        let(:result_queue) { result_work['queue'] }
        let(:result_resource) { result_work['resource'] }

        before { klass.perform_in(interval, *args) }

        around { |example| Timecop.freeze(now) { example.run } }

        it 'should enqueue the work to the default queue/resource' do
          expect(result_item).to include('queue' => queue_name, 'resource' => resource_name, 'class' => 'Sidekiq::Bolt::MockWorker', 'args' => args)
        end

        it 'should schedule to run in the specified interval' do
          expect(global_redis.zscore('bolt:scheduled', scheduled_job)).to eq(now.to_f + interval)
        end

        it 'should save the queue for later scheduling' do
          expect(result_queue).to eq(queue_name)
        end

        it 'should save the resource for later scheduling' do
          expect(result_resource).to eq(resource_name)
        end

        context 'with an overridden resource name' do
          let(:klass) { MockWorkerTwo }
          let(:resource_name) { MockWorkerTwo::RESOURCE_NAME }

          it 'should enqueue the work to the specified resource' do
            expect(result_item).to include('queue' => queue_name, 'resource' => resource_name, 'class' => 'Sidekiq::Bolt::MockWorkerTwo', 'args' => args)
          end

          it 'should save the resource for later scheduling' do
            expect(result_resource).to eq(resource_name)
          end
        end

        context 'with an overridden queue name' do
          let(:klass) { MockWorkerThree }
          let(:queue_name) { MockWorkerThree::QUEUE_NAME }

          it 'should enqueue the work to the specified queue' do
            expect(result_item).to include('queue' => queue_name, 'resource' => resource_name, 'class' => 'Sidekiq::Bolt::MockWorkerThree', 'args' => args)
          end

          it 'should save the queue for later scheduling' do
            expect(result_queue).to eq(queue_name)
          end
        end
      end

      describe '.perform_async_with_options' do
        let(:options) { {} }
        before { klass.perform_async_with_options(options, *args) }

        it 'should enqueue the work to the default queue/resource' do
          expect(result_item).to include('queue' => queue_name, 'resource' => resource_name, 'class' => 'Sidekiq::Bolt::MockWorker', 'args' => args)
        end

        context 'when the class sets the resource name' do
          let(:resource_name) { MockWorkerTwo::RESOURCE_NAME }
          let(:klass) { MockWorkerTwo }

          it 'should enqueue the work to the proper resource' do
            expect(result_item['resource']).to eq(resource_name)
          end
        end

        context 'when the class sets the queue name' do
          let(:klass) { MockWorkerThree }

          it 'should enqueue the work to the proper resource' do
            expect(result_item['queue']).to eq(MockWorkerThree::QUEUE_NAME)
          end
        end

        context 'with an overridden queue' do
          let(:new_queue) { Faker::Lorem.word }
          let(:options) { {queue: new_queue} }

          it 'should use the new queue' do
            expect(result_item['queue']).to eq(new_queue)
          end
        end

        context 'with an overridden resource' do
          let(:new_resource) { Faker::Lorem.word }
          let(:resource_name) { new_resource }
          let(:options) { {resource: new_resource} }

          it 'should use the new queue' do
            expect(result_item['resource']).to eq(new_resource)
          end
        end
      end

    end
  end
end
