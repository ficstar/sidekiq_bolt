require 'rspec'

module Sidekiq
  module Bolt
    describe Serializer do

      class MockSerializer
        extend Serializer
      end

      let(:value) { [rand(0...10)] }

      describe 'serialization and deserialization' do
        let(:dumped_value) { MockSerializer.dump_json(value) }

        subject { MockSerializer.load_json(dumped_value) }

        it { is_expected.to eq(value) }

        context 'with a string value' do
          let(:value) { Faker::Lorem.word }
          it { is_expected.to eq(value) }
        end

        context 'with a complex object' do
          let(:value) { RSpec }
          it { is_expected.to eq(value) }
        end

        context 'with serialized data the provides an invalid MAGIC file header' do
          let(:dumped_value) { 'MUSH' + Marshal.dump(value) }
          it { expect{subject}.to raise_error('Invalid Marshal dump provided') }
        end

        context 'with the proper MAGIC provided' do
          let(:dumped_value) { 'MRSH'.encode('ASCII-8BIT') + Marshal.dump(value) }
          it { is_expected.to eq(value) }
        end

        context 'when the dumped value is not a string' do
          let(:dumped_value) { String }
          it { expect{subject}.to raise_error('Invalid Marshal dump provided') }
        end
      end

    end
  end
end
