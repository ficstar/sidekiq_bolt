require 'rspec'

module Sidekiq
  module Bolt
    describe FutureWorker do

      MockFuture = Struct.new(:error) do
        def on_complete
          yield nil, error
        end
      end

      module FutureWorker
        class MockWorker
          include FutureWorker
          sidekiq_options resource: Faker::Lorem.word
          define_method(:perform_future) { |*_| MockFuture.new }
        end

        class MockErrorWorker
          include FutureWorker
          sidekiq_options resource: Faker::Lorem.word
          define_method(:perform_future) { |*args| MockFuture.new(StandardError.new(args.inspect)) }
        end
      end

      let(:worker_class) { FutureWorker::MockWorker }
      let(:worker) { worker_class.new }
      let(:args) { Faker::Lorem.paragraphs }
      let(:resource) { Resource.new(Resource::ASYNC_LOCAL_RESOURCE) }
      let(:sidekiq_options) { {concurrency: 0} }

      subject { worker }

      before do
        Bolt.configure_server(Sidekiq)
        allow(worker).to receive(:acknowledge_work)
      end

      it { is_expected.to be_a_kind_of(Worker) }

      describe '.perform_async' do
        it 'should run the work locally regardless of sidekiq options' do
          expect_any_instance_of(worker_class).to receive(:perform).with(*args)
          worker_class.perform_async(*args)
        end
      end

      describe '.perform_async_with_options' do
        it 'should run the work locally regardless of sidekiq options' do
          expect_any_instance_of(worker_class).to receive(:perform).with(*args)
          worker_class.perform_async_with_options({resource: Faker::Lorem.word}, *args)
        end
      end

      describe '.perform_in' do
        it 'should raise an error indicating that it is not supported' do
          expect { worker_class.perform_in(120, *args) }.to raise_error(NotImplementedError, '.peform_in not implemented for FutureWorkers')
        end
      end

      describe '#perform' do
        it 'should acknowledge the work' do
          expect(worker).to receive(:acknowledge_work)
          subject.perform(*args)
        end

        context 'when the future returns an error' do
          let(:worker_class) { FutureWorker::MockErrorWorker }

          it 'should acknowledge the work with the error' do
            expect(worker).to receive(:acknowledge_work) do |error|
              expect(error).to be_a_kind_of(StandardError)
              expect(error.to_s).to eq(args.inspect)
            end
            subject.perform(*args)
          end
        end
      end

    end
  end
end
