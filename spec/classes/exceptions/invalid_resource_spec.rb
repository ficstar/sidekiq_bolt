require 'rspec'

module Sidekiq
  module Bolt
    module Exceptions
      describe InvalidResource do

        let(:resource) { Faker::Lorem.sentence }

        subject { InvalidResource.new(resource) }

        it { is_expected.to be_a_kind_of(StandardError) }

        its(:message) { is_expected.to eq(%Q{Resource "#{resource}" has been invalidated!}) }
        its(:resource) { is_expected.to eq(resource) }

      end
    end
  end
end
