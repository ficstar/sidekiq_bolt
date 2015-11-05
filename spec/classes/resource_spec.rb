require 'rspec'

module Sidekiq
  module Bolt
    describe Resource do

      let(:name) { 'resourceful' }

      subject { Resource.new(name) }

      describe '#type' do
        let(:type) { 'zippers' }

        before { subject.type = type }

        it 'should be the specified type' do
          expect(subject.type).to eq('zippers')
        end

        it 'should store the value in redis' do
          expect(global_redis.get('resource:type:resourceful')).to eq('zippers')
        end
      end

      describe '#limit' do
        let(:limit) { 5 }

        before { subject.limit = limit }

        it 'should be the specified limit' do
          expect(subject.limit).to eq(5)
        end

        it 'should store the value in redis' do
          expect(global_redis.get('resource:limit:resourceful')).to eq('5')
        end

        it 'should be an integer property' do
          expect(Resource.new(name).limit).to eq(5)
        end
      end

      describe '#allocated' do
        let(:busy) { 5 }

        before { global_redis.set("resource:allocated:#{name}", busy) }

        its(:allocated) { is_expected.to eq(5) }

        context 'with a different resource' do
          let(:busy) { 15 }
          let(:name) { 'really_busy' }

          its(:allocated) { is_expected.to eq(15) }
        end
      end

    end
  end
end
