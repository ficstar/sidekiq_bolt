module Sidekiq
  module Bolt
    class PersistantResource < Struct.new(:name)

      def create(resource)
        Bolt.redis do |redis|
          redis.lpush("resources:persistant:#{name}", resource)
          resource
        end
      end

    end
  end
end
