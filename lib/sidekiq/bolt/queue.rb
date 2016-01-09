module Sidekiq
  module Bolt
    class Queue < Struct.new(:name)

      def self.all
        Bolt.redis do |conn|
          conn.smembers('queues')
        end.map { |name| new(name) }
      end

      def resources
        Bolt.redis do |conn|
          conn.smembers("queue:resources:#{name}")
        end.map { |name| Resource.new(name) }
      end

      def paused=(value)
        Bolt.redis do |redis|
          if value
            redis.set("queue:paused:#{name}", 1)
          else
            redis.del("queue:paused:#{name}")
          end
        end
      end

      def blocked=(value)
        Bolt.redis do |redis|
          if value
            redis.set("queue:blocked:#{name}", 1)
          else
            redis.del("queue:blocked:#{name}")
          end
        end
      end

      def size
        resources.map(&:size).reduce(&:+) || 0
      end

      def retrying
        resources.map(&:retrying).reduce(&:+) || 0
      end

      def busy
        resources.map(&:allocated).reduce(&:+) || 0
      end

      def enqueue(resource_name, workload, retrying = false)
        Resource.new(resource_name).add_work(name, workload, retrying)
      end

    end
  end
end
