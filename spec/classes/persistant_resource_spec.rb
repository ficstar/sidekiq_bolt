require 'rspec'

module Sidekiq
  module Bolt
    describe PersistantResource do

      let(:name) { SecureRandom.uuid }

      subject { PersistantResource.new(name) }

      describe '#create' do
        let(:resource) { SecureRandom.uuid }
        let!(:item) { subject.create(resource) }
        let(:result_items) { global_redis.zrangebyscore("resources:persistant:#{name}", '-INF', '-INF') }

        it 'should add an item to this resource' do
          expect(result_items).to include(resource)
        end

        it 'should return the resource' do
          expect(item).to eq(resource)
        end

        context 'when called multiple times' do
          let(:resource_two) { SecureRandom.uuid }
          let!(:item_two) { subject.create(resource_two) }

          it 'should add the first item' do
            expect(result_items).to include(resource)
          end

          it 'should add the second item' do
            expect(result_items).to include(resource_two)
          end
        end
      end

    end
  end
end
