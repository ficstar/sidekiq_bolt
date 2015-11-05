module Sidekiq
  module Bolt
    class Resource < Struct.new(:name)
      extend PropertyList

      define_property 'resource:type', :type
      define_property 'resource:limit', :limit, :int

      def allocated
        Bolt.redis { |redis| redis.get(allocated_key).to_i }
      end

      private

      def allocated_key
        "resource:allocated:#{name}"
      end

    end
  end
end
