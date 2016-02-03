require 'rspec'

module Sidekiq
  module Bolt
    describe PersistentResource do

      let(:resource) { SecureRandom.uuid }
      let(:name) { SecureRandom.uuid }
      let(:persistent_resource) { PersistentResource.new(name) }

      subject { persistent_resource }

      describe '#create' do
        let!(:item) { subject.create(resource) }
        let(:result_items) { global_redis.zrangebyscore("resources:persistent:#{name}", '-INF', '-INF') }

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

      describe '#allocate' do
        let(:score) { '-INF' }

        subject { persistent_resource.allocate }

        before do
          global_redis.zadd("resources:persistent:#{name}", score, resource)
        end

        it { is_expected.to eq(resource) }

        context 'with an item having a better score' do
          let!(:score) { 51.3 }
          let!(:score_two) { 0.0 }
          let(:resource_two) { SecureRandom.uuid }

          before do
            global_redis.zadd("resources:persistent:#{name}", score_two, resource_two)
          end

          it { is_expected.to eq(resource_two) }
        end

        context 'when the score of the only item is really high for some reason' do
          let(:score) { 'INF' }

          it { is_expected.to eq(resource) }
        end
      end

    end
  end
end
