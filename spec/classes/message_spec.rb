require 'rspec'

module Sidekiq
  module Bolt
    describe Message do

      let(:valid_attributes) { Message::VALID_ATTRIBUTES }
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

        context 'with all attributes present' do
          before { subject.marshal_load(message.marshal_dump) }

          it { is_expected.to eq(message_attributes.slice(*valid_attributes)) }
        end

        context 'when an attribute is missing' do
          let(:missing_attribute) { valid_attributes.sample }
          let(:expected_attribute_slice) { valid_attributes - [missing_attribute] }

          before do
            message.delete(missing_attribute)
            subject.marshal_load(message.marshal_dump)
          end

          it { is_expected.to eq(message_attributes.slice(*expected_attribute_slice)) }
        end

        context 'when a "false" attribute' do
          let(:false_attribute) { valid_attributes.sample }

          before do
            message_attributes[false_attribute] = false
            message[false_attribute] = false
            subject.marshal_load(message.marshal_dump)
          end

          it { is_expected.to eq(message_attributes.slice(*valid_attributes)) }
        end
      end

    end
  end
end
