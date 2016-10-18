require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch::UnitOfWork do

      let(:queue) { Faker::Lorem.word }
      let(:allocation) { rand(1..100) }
      let(:resource_type) { Faker::Lorem.word }
      let(:resource_name) { Faker::Lorem.word }
      let(:work) { SecureRandom.uuid }
      let(:sidekiq_options) { {concurrency: 1, concurrency_pool: {resource_type => 1}} }

      before do
        Resource.new(resource_name).type = resource_type
        Fetch.processor_allocator.allocate(1, resource_type)
      end

      subject { Fetch::UnitOfWork.new(queue, allocation, resource_name, work) }

      its(:queue) { is_expected.to eq(queue) }
      its(:allocation) { is_expected.to eq(allocation) }
      its(:queue_name) { is_expected.to eq(queue) }
      its(:resource_name) { is_expected.to eq(resource_name) }
      its(:job) { is_expected.to eq(work) }
      its(:message) { is_expected.to eq(work) }

      describe '#processor_type' do
        let(:processor_type) { Faker::Lorem.word }
        before { subject.processor_type = processor_type }
        its(:processor_type) { is_expected.to eq(processor_type) }
      end

      shared_examples_for 'freeing up a resource' do |method|
        it 'should free the work from the resource' do
          expect_any_instance_of(Resource).to receive(:free) do |resource, queue_name, allocation, job|
            expect(resource.name).to eq(resource_name)
            expect(queue_name).to eq(queue)
            expect(allocation).to eq(allocation)
            expect(job).to eq(work)
          end
          subject.public_send(method)
        end

        it 'should free the worker' do
          subject.public_send(method)
          expect(Fetch.processor_allocator.allocation(resource_type)).to be_zero
        end
      end

      shared_examples_for 'acknowledging work' do |method|
        it_behaves_like 'freeing up a resource', method

        context 'when the resource is "$async_local"' do
          let(:resource_name) { Resource::ASYNC_LOCAL_RESOURCE }

          it 'should not free the work' do
            expect_any_instance_of(Resource).not_to receive(:free)
            subject.public_send(method)
          end
        end
      end

      describe '#acknowledge' do
        it_behaves_like 'acknowledging work', :acknowledge
      end

      describe '#force_acknowledge' do
        it_behaves_like 'freeing up a resource', :acknowledge
      end

      describe '#requeue' do
        it 'adds the work back into the resource' do
          expect_any_instance_of(Resource).to receive(:add_work) do |resource, queue_name, job|
            expect(resource.name).to eq(resource_name)
            expect(queue_name).to eq(queue)
            expect(job).to eq(work)
          end
          subject.requeue
        end

        it_behaves_like 'acknowledging work', :requeue
      end

    end
  end
end
