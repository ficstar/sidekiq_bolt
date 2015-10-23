module Sidekiq
  module Bolt
    module PropertyList

      def define_property(namespace, property)
        define_method("#{property}=") do |value|
          Bolt.redis { |conn| conn.set("#{namespace}:#{property}", value) }
        end
        define_method(property) do
          Bolt.redis { |conn| conn.get("#{namespace}:#{property}") }
        end
      end

    end
  end
end
