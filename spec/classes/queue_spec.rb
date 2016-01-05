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

      describe '#resources' do
        let(:resource) { Resource.new(Faker::Lorem.word) }

        before { resource.add_work(name, '') }

        its(:resources) { is_expected.to eq([resource]) }

        context 'with multiple resources' do
          let(:resource_two) { Resource.new(Faker::Lorem.word) }
          let(:name_two) { name }

          before { resource_two.add_work(name_two, '') }

          its(:resources) { is_expected.to match_array([resource, resource_two]) }

          context 'when the resource does not associate with this queue' do
            let(:name_two) { Faker::Lorem.word }

            its(:resources) { is_expected.to eq([resource]) }
          end
        end
      end

    end
  end
end
