require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch::UnitOfWork do

      let(:queue) { Faker::Lorem.word }
      let(:resource_name) { Faker::Lorem.word }
      let(:work) { SecureRandom.uuid }

      subject { Fetch::UnitOfWork.new(queue, resource_name, work) }

      its(:queue) { is_expected.to eq(queue) }
      its(:queue_name) { is_expected.to eq(queue) }
      its(:resource) { is_expected.to eq(resource_name) }
      its(:job) { is_expected.to eq(work) }

      shared_examples_for 'acknowledging work' do |method|
        it 'should free the work from the resource' do
          expect_any_instance_of(Resource).to receive(:free) do |resource, queue_name, job|
            expect(resource.name).to eq(resource_name)
            expect(queue_name).to eq(queue)
            expect(job).to eq(work)
          end
          subject.public_send(method)
        end
      end

      describe '#acknowledge' do
        it_behaves_like 'acknowledging work', :acknowledge
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