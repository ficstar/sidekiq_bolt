require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe TypeSafety do

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:queue) { Queue.new(queue_name) }
          let(:args) { Faker::Lorem.paragraphs }
          let(:job) { {'queue' => queue_name, 'args' => args.dup} }
          let(:yield_result) { Faker::Lorem.word }
          let(:worker) { nil }

          it_behaves_like 'a server middleware'

          it 'should leave the args alone' do
            subject.call(nil, job, nil) {}
            expect(job['args']).to eq(args)
          end

          context 'when an encoded time object is present' do
            let(:args) { [EncodedTime.new(543535.22)] }

            before { subject.call(nil, job, nil) {} }

            it 'should convert it to a Time object' do
              expect(job['args'].first).to be_a_kind_of(Time)
            end

            it 'should save the right time' do
              expect(job['args'].first).to eq(args.first.to_time)
            end

            context 'when the time parameter is somewhere else' do
              let(:args) { [*Faker::Lorem.paragraphs, EncodedTime.new(543535.22)] }

              it 'should convert it to an EncodedTime' do
                expect(job['args'].last).to be_a_kind_of(Time)
              end

              it 'should save the right time' do
                expect(job['args'].last).to eq(args.last.to_time)
              end
            end
          end
        end

      end
    end
  end
end
