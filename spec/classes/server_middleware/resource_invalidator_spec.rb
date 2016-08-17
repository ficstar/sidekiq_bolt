require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe ResourceInvalidator do

        describe '#call' do
          let(:queue_name) { Faker::Lorem.word }
          let(:queue) { Queue.new(queue_name) }
          let(:args) { Faker::Lorem.paragraphs }
          let(:job) { Message['queue' => queue_name, 'args' => args.dup] }
          let(:yield_result) { Faker::Lorem.word }
          let(:worker) { nil }

          it_behaves_like 'a server middleware'

          context 'when the block raises an exception' do
            let(:resource_type) { Faker::Lorem.word }
            let(:resource_allocator) { PersistentResource.new(resource_type) }
            let(:resource) { Faker::Lorem.sentence }
            let(:allocated_resource) { JSON.dump(resource: resource_type, item: resource) }
            let(:error) { Exceptions::InvalidResource.new(resource_allocator, resource) }
            let(:result_resources) { global_redis.lrange("resources:persistent:backup:worker:#{worker}", 0, -1) }
            let(:worker) { SecureRandom.uuid }

            before do
              allow(resource_allocator).to receive(:identity).and_return(worker)
              resource_allocator.create(resource)
              resource_allocator.allocate
              subject.call(nil, job, nil) { raise error } rescue nil
            end

            it 'should should remove this resource from backup' do
              expect(result_resources).not_to include(allocated_resource)
            end

            it 'should re-raise the error' do
              expect { subject.call(nil, job, nil) { raise error }.get }.to raise_error(error)
            end
          end

        end

      end
    end
  end
end
