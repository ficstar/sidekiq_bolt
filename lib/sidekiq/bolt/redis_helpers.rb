module Sidekiq
  module Bolt

    def self.redis(&block)
      @redis ||= begin
        ConnectionPool.new do
          conn = Redis.new(host: 'redis.dev', db: 13)
          Redis::Namespace.new(:bolt, redis: conn)
        end
      end
      @redis.with(&block)
    end

  end
end
