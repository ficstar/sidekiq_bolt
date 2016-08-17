require 'rspec'

module Sidekiq
  module Bolt
    module ClientMiddleware
      describe TypeSafety do

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:queue) { Queue.new(queue_name) }
          let(:args) { Faker::Lorem.paragraphs }
          let(:job) { Message['queue' => queue_name, 'args' => args.dup] }
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

          it 'should leave the args alone' do
            subject.call(nil, job, nil) {}
            expect(job['args']).to eq(args)
          end

          context 'when a time object is present' do
            let(:args) { [Time.at(543535.22)] }

            before { subject.call(nil, job, nil) {} }

            it 'should convert it to an EncodedTime' do
              expect(job['args'].first).to be_a_kind_of(EncodedTime)
            end

            it 'should save the right time' do
              expect(job['args'].first.to_time).to eq(args.first)
            end

            context 'when the time parameter is somewhere else' do
              let(:args) { [*Faker::Lorem.paragraphs, Time.at(543535.22)] }

              it 'should convert it to an EncodedTime' do
                expect(job['args'].last).to be_a_kind_of(EncodedTime)
              end

              it 'should save the right time' do
                expect(job['args'].last.to_time).to eq(args.last)
              end
            end
          end
        end

      end
    end
  end
end
