require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch do

      let(:options) { {} }

      subject { Fetch.new(options) }

      describe '.bulk_requeue' do
        subject { Fetch }
        it { is_expected.to respond_to(:bulk_requeue) }
      end

      describe '#retrieve_work' do
        before { allow_any_instance_of(Fetch).to receive(:sleep) }

        its(:retrieve_work) { is_expected.to be_nil }

        it 'should sleep 1 second to wait for more work' do
          expect(subject).to receive(:sleep).with(1)
          subject.retrieve_work
        end
      end

    end
  end
end
