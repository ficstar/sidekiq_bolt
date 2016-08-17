require 'rspec'

module Sidekiq
  module Bolt
    describe Message do

      it { is_expected.to be_a_kind_of(Hash) }

    end
  end
end
