require 'rspec'

module Sidekiq
  module Bolt
    describe FutureWorker do

      MockFuture = Struct.new(:error) do
        def on_complete
          yield nil, error
        end
      end

      let(:worker_class_base) do
        Class.new do
          include FutureWorker
          define_method(:perform_future) { |*args| MockFuture.new }
        end
      end
      let(:worker_class) { worker_class_base }
      let(:worker) { worker_class.new }
      let(:args) { Faker::Lorem.paragraphs }

      subject { worker }

      before { allow(worker).to receive(:acknowledge_work) }

      it { is_expected.to be_a_kind_of(Worker) }

      describe '#perform' do
        it 'should acknowledge the work' do
          expect(worker).to receive(:acknowledge_work)
          subject.perform(*args)
        end

        context 'when the future returns an error' do
          let(:worker_class) do
            Class.new(worker_class_base) do
              define_method(:perform_future) { |*args| MockFuture.new(StandardError.new(args.inspect)) }
            end
          end

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
