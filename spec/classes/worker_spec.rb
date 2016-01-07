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

      subject { MockWorker.new }

      it { is_expected.to be_a_kind_of(Sidekiq::Worker) }

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
