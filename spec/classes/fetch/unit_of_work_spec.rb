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

      describe '#acknowledge' do
        let(:resource) { Resource.new(resource_name) }

        before do
          resource.add_work(queue, work)
          resource.allocate(1)
        end

        it 'should free the work from the resource' do
          subject.acknowledge
          expect(resource.allocated).to eq(0)
        end
      end
    end
  end
end
