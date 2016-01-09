require 'rspec'

module Sidekiq
  module Bolt
    module ClientMiddleware
      describe BlockQueue do

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:queue) { Queue.new(queue_name) }
          let(:job) { {'queue' => queue_name} }
          let(:yield_result) { Faker::Lorem.word }

          it 'should yield' do
            expect { |block| subject.call(nil, job, nil, &block) }.to yield_control
          end

          it 'should support an optional redis pool' do
            expect { subject.call(nil, job, nil, nil) {} }.not_to raise_error
          end

          it 'should return the value of the block' do
            expect(subject.call(nil, job, nil) { yield_result }).to eq(yield_result)
          end

          context 'when the queue is blocked' do
            before { queue.blocked = true }

            it 'should not yield' do
              expect { |block| subject.call(nil, job, nil, &block) }.not_to yield_control
            end

            it 'should return false' do
              expect(subject.call(nil, job, nil, nil) { yield_result }).to eq(false)
            end
          end
        end

      end
    end
  end
end
