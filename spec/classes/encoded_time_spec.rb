require 'rspec'

module Sidekiq
  module Bolt
    describe EncodedTime do

      let(:now) { Time.at(8482344.33) }

      subject { EncodedTime.new(now.to_f) }

      around { |example| Timecop.freeze(now) { example.run } }

      its(:to_time) { is_expected.to eq(now) }

    end
  end
end
