module Sidekiq
  module Bolt
    module PropertyList

      def define_property(namespace, property, type = nil)
        instance_variable = :"@#{property}"

        define_method("#{property}=") do |value|
          Bolt.redis do |conn|
            instance_variable_set(instance_variable, value)
            conn.set("#{namespace}:#{name}", value)
          end
        end
        if type == :counter
          define_method("#{property}_incr") do
            Bolt.redis do |conn|
              conn.incr("#{namespace}:#{name}")
            end
          end
          define_method("#{property}_incrby") do |amount|
            Bolt.redis do |conn|
              conn.incrby("#{namespace}:#{name}", amount)
            end
          end
          define_method("#{property}_decr") do
            Bolt.redis do |conn|
              conn.decr("#{namespace}:#{name}")
            end
          end
          define_method("#{property}_decrby") do |amount|
            Bolt.redis do |conn|
              conn.decrby("#{namespace}:#{name}", amount)
            end
          end
        end
        define_method(property) do
          Bolt.redis do |conn|
            result = instance_variable_get(instance_variable)
            return result if result

            result = conn.get("#{namespace}:#{name}")
            result = result.to_i if [:int, :counter].include?(type)
            instance_variable_set(instance_variable, result)
          end
        end
      end

    end
  end
end
