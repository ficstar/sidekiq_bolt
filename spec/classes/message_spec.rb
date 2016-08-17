require 'rspec'

module Sidekiq
  module Bolt
    describe Message do

      let(:valid_attributes) { %w(class args queue resource jid pjid persist) }
      let(:invalid_attributes) { Faker::Lorem.words }
      let(:message_attributes) do
        (valid_attributes + invalid_attributes).inject({}) { |memo, key| memo.merge!(key => Faker::Lorem.word) }
      end
      let(:message) { Message[message_attributes] }

      subject { message }

      it { is_expected.to be_a_kind_of(Hash) }

      describe '#marshal_dump' do
        subject { message.marshal_dump }
        it { is_expected.to eq(message_attributes.values_at(*valid_attributes)) }
      end

      describe '#marshal_load' do
        subject { Message.new }

        before { subject.marshal_load(message.marshal_dump) }

        it { is_expected.to eq(message_attributes.slice(*valid_attributes)) }
      end

    end
  end
end
