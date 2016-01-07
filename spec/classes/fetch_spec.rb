require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch do

      describe '.bulk_requeue' do
        subject { Fetch }
        it { is_expected.to respond_to(:bulk_requeue) }
      end

    end
  end
end
