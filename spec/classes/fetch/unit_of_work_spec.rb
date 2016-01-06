require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch::UnitOfWork do

      let(:queue) { Faker::Lorem.word }
      let(:resource) { Faker::Lorem.word }
      let(:work) { SecureRandom.uuid }

      subject { Fetch::UnitOfWork.new(queue, resource, work) }

      its(:queue) { is_expected.to eq(queue) }
      its(:queue_name) { is_expected.to eq(queue) }
      its(:resource) { is_expected.to eq(resource) }
      its(:job) { is_expected.to eq(work) }

    end
  end
end
