module RedisHelpers
  extend RSpec::Core::SharedContext

  let(:redis_host) { 'redis.dev' }
  let(:global_namespace) { :bolt }
  let(:global_redis_db) { 13 }
  let(:global_redis_conn) { Redis.new(host: redis_host, db: global_redis_db) }
  let(:global_redis) { Redis::Namespace.new(global_namespace, redis: global_redis_conn) }

  before do
    Sidekiq.redis = {url: 'redis://redis.dev/13', namespace: :bolt}
    keys = global_redis.keys
    global_redis.pipelined do
      keys.each { |key| global_redis.del(key) }
    end
  end
end
