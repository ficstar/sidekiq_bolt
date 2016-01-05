module Sidekiq
  module Bolt
    class Queue < Struct.new(:name)

      def resources
        Bolt.redis do |conn|
          conn.smembers("queue:resources:#{name}")
        end.map { |name| Resource.new(name) }
      end

    end
  end
end
