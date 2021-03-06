module Sidekiq
  module Bolt
    class Job < Struct.new(:name)

      def self.all
        Bolt.redis { |redis| redis.smembers('jobs') }.map do |job_name|
          new(job_name)
        end
      end

      def add_queue(queue_name)
        Bolt.redis do |redis|
          redis.pipelined do
            redis.sadd("job:queues:#{name}", queue_name)
            redis.sadd('jobs', name)
            redis.set("queue:job:#{queue_name}", name)
          end
        end
      end

      def queues
        Bolt.redis { |redis| redis.smembers("job:queues:#{name}") }.map do |queue_name|
          Queue.new(queue_name)
        end
      end

    end
  end
end
