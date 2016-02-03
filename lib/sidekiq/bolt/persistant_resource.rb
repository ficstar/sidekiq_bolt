module Sidekiq
  module Bolt
    class PersistantResource < Struct.new(:name)

      def create(resource)
        Bolt.redis do |redis|
          redis.zadd("resources:persistant:#{name}", '-INF', resource)
          resource
        end
      end

    end
  end
end
