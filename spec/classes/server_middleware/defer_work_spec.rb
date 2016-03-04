require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe DeferWork do

        describe '#call' do
          let(:error) { nil }
          let(:valid_future) { double(:future) }
          let(:perform_future) { valid_future }
          let(:worker) { double(:worker, jid: SecureRandom.uuid, acknowledge_work: nil) }
          let(:defer) { false }
          let(:args) { Faker::Lorem.sentences }
          let(:job) { {'defer' => defer, 'args' => args} }

          before do
            allow(worker).to receive(:perform).with(*args).and_return(perform_future)
            allow(valid_future).to receive(:on_complete) do |&block|
              block.call(nil, error)
            end
          end

          it 'should yield' do
            expect { |block| subject.call(worker, job, nil, &block) }.to yield_control
          end

          context 'when the work is to be deferred' do
            let(:defer) { true }

            it 'should not yield' do
              expect { |block| subject.call(worker, job, nil, &block) }.not_to yield_control
            end

            it 'should perform the work' do
              expect(worker).to receive(:perform).with(*args)
              subject.call(worker, job, nil)
            end

            context 'when the future completes in failure' do
              let(:error) { StandardError.new(Faker::Lorem.sentence) }

              it 'should acknowledge the work with the error' do
                expect(worker).to receive(:acknowledge_work).with(error)
                subject.call(worker, job, nil)
              end
            end

            context 'when #perform does not return a value responding to #on_complete' do
              let(:perform_future) { Faker::Lorem.word }

              it 'should raise an error indicating that the result is invalid' do
                expect { subject.call(worker, job, nil) }.to raise_error('Expected worker to return a future!')
              end
            end

            context 'when the worker modifies the input args' do
              let(:args) { [a: :b] }

              before do
                allow(worker).to receive(:perform) do |some_hash|
                  some_hash[:c] = '1337n355'
                  perform_future
                end
              end

              it 'should not mangle the original arguments' do
                subject.call(worker, job, nil)
                expect(job['args']).to eq([a: :b])
              end

            end
          end

        end

      end
    end
  end
end
