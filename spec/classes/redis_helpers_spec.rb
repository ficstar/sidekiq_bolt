require 'rspec'

module Sidekiq
  module Bolt
    describe RedisHelpers do

      describe '.redis' do
        it 'should yield the bolt namespaced redis connection' do
          Bolt.redis { |conn| conn.set('hello', 'world') }
          expect(global_redis.get('hello')).to eq('world')
        end

        context 'when called multiple times' do
          it 'should re-use the same connection' do
            conn = Bolt.redis { |conn| conn }
            other_conn = Bolt.redis { |conn| conn }
            expect(other_conn).to eq(conn)
          end
        end

        context 'with multiple threads' do
          it 'should use a connection pool' do
            waiting = true
            started = false
            worker = Thread.start do
              Bolt.redis do |conn|
                started = true
                sleep 0.01 while waiting
                conn
              end
            end
            sleep 0.01 until started
            other_conn = Bolt.redis { |conn| conn }
            #noinspection RubyUnusedLocalVariable
            waiting = false
            conn = worker.value
            expect(other_conn).not_to eq(conn)
          end
        end

        context 'with a different redis connection configured through sidekiq' do
          before { Sidekiq.redis = {url: 'redis://redis.dev/12', namespace: :bolty} }

          it 'should yield the configured connection' do
            Bolt.redis { |conn| conn.set('hello', 'world') }
            result = Sidekiq.redis { |conn| conn.get('hello') }
            expect(result).to eq('world')
          end
        end
      end

    end
  end
end
