module Sidekiq
  module Bolt
    class PersistentResource < Struct.new(:name)
      include Sidekiq::Util

      NAMESPACE_KEY = [''].freeze
      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      ALLOCATE_SCRIPT_PATH = "#{SCRIPT_ROOT}/alloc.lua"
      ALLOCATE_SCRIPT = File.read(ALLOCATE_SCRIPT_PATH)

      def initialize(name, redis_pool = nil)
        @redis_pool = redis_pool
        super(name)
      end

      def create(resource)
        redis do |redis|
          redis.zadd("resources:persistent:#{name}", '-INF', resource)
          resource
        end
      end

      def size
        redis { |redis| redis.zcard("resources:persistent:#{name}") }
      end

      def destroy(resource)
        backup_resource = JSON.dump(resource: name, item: resource)
        redis do |redis|
          redis.zrem("resources:persistent:#{name}", resource)
          redis.lrem("resources:persistent:backup:worker:#{identity}", 0, backup_resource)
          resource
        end
      end

      def allocate
        redis do |redis|
          redis.eval(ALLOCATE_SCRIPT, keys: NAMESPACE_KEY, argv: [name, identity])
        end
      end

      def free(resource, score)
        backup_resource = JSON.dump(resource: name, item: resource)
        redis do |redis|
          redis.pipelined do
            redis.zadd("resources:persistent:#{name}", score, resource)
            redis.lrem("resources:persistent:backup:worker:#{identity}", 0, backup_resource)
          end
        end
      end

      private

      def redis(&block)
        @redis_pool ? @redis_pool.with(&block) : Bolt.redis(&block)
      end

    end
  end
end
