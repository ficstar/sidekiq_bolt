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

      def busy
        resources.map(&:allocated).reduce(&:+) || 0
      end

      def enqueue(resource_name, workload, retrying = false)
        Resource.new(resource_name).add_work(name, workload, retrying)
      end

    end
  end
end
