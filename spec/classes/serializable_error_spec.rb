require 'rspec'

module Sidekiq
  module Bolt
    describe SerializableError do

      let(:base_error) { Interrupt.new }

      subject { SerializableError.new(base_error) }

      it { is_expected.to be_a_kind_of(StandardError) }

      its(:error_class) { is_expected.to eq(Interrupt) }
      its(:message) { is_expected.to eq(base_error.message) }
      its(:backtrace) { is_expected.to be_nil }

      context 'with a backtrace' do
        before do
          begin
            raise base_error
          rescue Exception => error
            base_error.set_backtrace(error.backtrace)
          end
        end

        its(:backtrace) { is_expected.to eq(base_error.backtrace) }
      end

      context 'with a different error type' do
        let(:message) { Faker::Lorem.sentence }
        let(:base_error) { StandardError.new(message) }

        its(:error_class) { is_expected.to eq(StandardError) }
        its(:message) { is_expected.to eq(message) }
      end

    end
  end
end
