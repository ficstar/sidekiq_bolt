require 'rspec'

module Sidekiq
  module Bolt
    describe PersistentResource do

      let(:resource) { SecureRandom.uuid }
      let(:name) { SecureRandom.uuid }
      let(:backup_resource) { JSON.dump(resource: name, item: resource) }
      let(:persistent_resource) { PersistentResource.new(name) }
      let(:worker) { SecureRandom.uuid }
      let(:redis_conn) { global_redis }

      subject { persistent_resource }

      before { allow(persistent_resource).to receive(:identity).and_return(worker) }

      it { is_expected.to be_a_kind_of(Sidekiq::Util) }

      describe '#create' do
        let!(:item) { subject.create(resource) }
        let(:result_items) { redis_conn.zrangebyscore("resources:persistent:#{name}", '-inf', '-inf') }

        it 'should add an item to this resource' do
          expect(result_items).to include(resource)
        end

        it 'should return the resource' do
          expect(item).to eq(resource)
        end

        context 'when provided with a redis pool' do
          let(:redis_conn) { alternate_redis }
          let(:redis_pool) { ConnectionPool.new { alternate_redis } }
          let(:persistent_resource) { PersistentResource.new(name, redis_pool) }

          it 'should add the item on the provided connection' do
            expect(result_items).to include(resource)
          end
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

      describe '#size' do
        let(:count) { 1 }

        before { count.times { subject.create(SecureRandom.uuid) } }

        its(:size) { is_expected.to eq(1) }

        context 'with a different number of resources' do
          let(:count) { 37 }
          its(:size) { is_expected.to eq(37) }
        end

        context 'when provided with a redis pool' do
          let(:redis_conn) { alternate_redis }
          let(:redis_pool) { ConnectionPool.new { alternate_redis } }
          let(:persistent_resource) { PersistentResource.new(name, redis_pool) }

          its(:size) { is_expected.to eq(1) }
        end
      end

      describe '#destroy' do
        let(:result_items) { redis_conn.zrangebyscore("resources:persistent:#{name}", '-inf', '-inf') }
        let(:item) { subject.destroy(resource) }

        before do
          subject.create(resource)
        end

        it 'should remove the resource from the pool' do
          subject.destroy(resource)
          expect(result_items).not_to include(resource)
        end

        it 'should return the resource' do
          expect(item).to eq(resource)
        end

        context 'when provided with a redis pool' do
          let(:redis_conn) { alternate_redis }
          let(:redis_pool) { ConnectionPool.new { alternate_redis } }
          let(:persistent_resource) { PersistentResource.new(name, redis_pool) }

          it 'should remove the resource from the pool on the provided connection' do
            subject.destroy(resource)
            expect(result_items).not_to include(resource)
          end
        end

        context 'when this resource is allocated' do
          let(:result_items) { redis_conn.lrange("resources:persistent:backup:worker:#{worker}", 0, -1) }
          let(:serialized_resource) { JSON.dump(resource: name, item: resource) }

          before { subject.allocate }

          it 'should remove the item from our backup list' do
            subject.destroy(resource)
            expect(result_items).not_to include(serialized_resource)
          end

          context 'with multiple allocated resource' do
            let(:resource_two) { SecureRandom.uuid }
            let(:serialized_resource_two) { JSON.dump(resource: name, item: resource_two) }

            before do
              subject.create(resource_two)
              subject.allocate
            end

            it 'should only remove the one resource' do
              subject.destroy(resource)
              expect(result_items).to include(serialized_resource_two)
            end
          end
        end
      end

      describe '#allocate' do
        let(:score) { '-inf' }

        subject { persistent_resource.allocate }

        before do
          redis_conn.zadd("resources:persistent:#{name}", score, resource) if resource
        end

        it { is_expected.to eq([resource, score]) }

        context 'with no resources available' do
          let(:resource) { nil }
          it { is_expected.to be_nil }
        end

        it 'should remove the item from persistence' do
          subject
          expect(redis_conn.zrange("resources:persistent:#{name}", 0, -1)).to be_empty
        end

        context 'when provided with a redis pool' do
          let(:redis_conn) { alternate_redis }
          let(:redis_pool) { ConnectionPool.new { alternate_redis } }
          let(:persistent_resource) { PersistentResource.new(name, redis_pool) }

          it 'should remove the item from persistence on the specified connection' do
            subject
            expect(redis_conn.zrange("resources:persistent:#{name}", 0, -1)).to be_empty
          end
        end

        it 'should back up the item into a list identified by the worker' do
          subject
          expect(redis_conn.lrange("resources:persistent:backup:worker:#{worker}", 0, -1)).to include(backup_resource)
        end

        context 'with an item having a better score' do
          let!(:score) { 51.3 }
          let!(:score_two) { 0.0 }
          let(:resource_two) { SecureRandom.uuid }

          before do
            redis_conn.zadd("resources:persistent:#{name}", score_two, resource_two)
          end

          it { is_expected.to eq([resource_two, score_two]) }

          it 'should remove only the best item from persistence' do
            subject
            expect(redis_conn.zrange("resources:persistent:#{name}", 0, -1)).to include(resource)
          end
        end

        context 'when the score of the only item is really high for some reason' do
          let(:score) { 'inf' }

          it { is_expected.to eq([resource, score]) }
        end
      end

      describe '#free' do
        let(:score) { rand(0...1000).to_f }

        before do
          subject.create(resource)
          subject.allocate
        end

        it 'should re-submit the resource to the pool' do
          subject.free(resource, score)
          expect(redis_conn.zrange("resources:persistent:#{name}", 0, -1)).to include(resource)
        end

        it 'should add it using the specified score' do
          subject.free(resource, score)
          expect(redis_conn.zscore("resources:persistent:#{name}", resource)).to eq(score)
        end

        context 'when provided with a redis pool' do
          let(:redis_conn) { alternate_redis }
          let(:redis_pool) { ConnectionPool.new { alternate_redis } }
          let(:persistent_resource) { PersistentResource.new(name, redis_pool) }

          it 'should re-submit the resource to the pool on the right connection' do
            subject.free(resource, score)
            expect(redis_conn.zrange("resources:persistent:#{name}", 0, -1)).to include(resource)
          end
        end

        it 'should remove the resource from the backup' do
          subject.free(resource, score)
          expect(redis_conn.lrange("resources:persistent:backup:worker:#{worker}", 0, -1)).not_to include(backup_resource)
        end

        context 'with multiple allocated items' do
          let(:resource_two) { SecureRandom.uuid }
          let(:backup_resource_two) { JSON.dump(resource: name, item: resource_two) }

          before do
            subject.create(resource_two)
            subject.allocate
          end

          it 'should only remove the freed resource' do
            subject.free(resource, score)
            expect(redis_conn.lrange("resources:persistent:backup:worker:#{worker}", 0, -1)).to include(backup_resource_two)
          end
        end
      end

    end
  end
end
