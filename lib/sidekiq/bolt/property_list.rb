module Sidekiq
  module Bolt
    module PropertyList

      def define_property(namespace, property)
        instance_variable = :"@#{property}"

        define_method("#{property}=") do |value|
          Bolt.redis do |conn|
            instance_variable_set(instance_variable, value)
            conn.set("#{namespace}:#{property}", value)
          end
        end
        define_method(property) do
          Bolt.redis do |conn|
            result = instance_variable_get(instance_variable)
            return result if result

            result = conn.get("#{namespace}:#{property}")
            instance_variable_set(instance_variable, result)
          end
        end
      end

    end
  end
end
