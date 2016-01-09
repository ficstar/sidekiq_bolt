require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe JobMetaData do

        describe '#call' do
          let(:worker) { Struct.new(:queue, :resource).new }
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          #noinspection RubyStringKeysInHashInspection
          let(:job) { {'queue' => queue_name, 'resource' => resource_name} }

          it 'should yield' do
            expect { |block| subject.call(worker, job, nil, &block) }.to yield_control
          end

          describe 'setting meta data' do
            let(:middleware) { JobMetaData.new }
            subject { worker }
            before { middleware.call(worker, job, nil) {} }

            its(:queue) { is_expected.to eq(Queue.new(queue_name)) }
            its(:resource) { is_expected.to eq(Resource.new(resource_name)) }
          end

        end

      end
    end
  end
end
