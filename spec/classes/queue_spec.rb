require 'rspec'

module Sidekiq
  module Bolt
    describe Queue do

      let(:name) { Faker::Lorem.word }
      let(:queue) { Queue.new(name) }

      subject { queue }

      describe '#name' do
        its(:name) { is_expected.to eq(name) }
      end

    end
  end
end
