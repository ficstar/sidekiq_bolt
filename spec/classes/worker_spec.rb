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

      subject { MockWorker.new }

      it { is_expected.to be_a_kind_of(Sidekiq::Worker) }

      describe '.perform_async' do
        let(:args) { Faker::Lorem.paragraphs }
        let(:queue_name) { 'default' }
        let(:resource_name) { 'default' }
        let(:resource) { Resource.new(resource_name) }
        let(:result_work) { resource.allocate(1) }
        let(:result_item) { Sidekiq.load_json(result_work[1]) }
        let(:klass) { MockWorker }

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

    end
  end
end
