module Sidekiq
  module Bolt
    module PropertyList

      def define_property(namespace, property, type = nil)
        instance_variable = :"@#{property}"

        defined_setter(instance_variable, namespace, property)
        define_getter(instance_variable, namespace, property, type)
        define_counter_methods(namespace, property) if type == :counter
      end

      private

      def define_counter_methods(namespace, property)
        define_counter_method(namespace, property, :incr)
        define_counter_method(namespace, property, :incrby)
        define_counter_method(namespace, property, :decr)
        define_counter_method(namespace, property, :decrby)
      end

      def define_counter_method(namespace, property, redis_method)
        define_method("#{property}_#{redis_method}") do |*args|
          Bolt.redis do |conn|
            conn.public_send(redis_method, "#{namespace}:#{name}", *args)
          end
        end
      end

      def define_getter(instance_variable, namespace, property, type)
        define_method(property) do
          Bolt.redis do |conn|
            result = instance_variable_get(instance_variable)
            return result if result

            result = conn.get("#{namespace}:#{name}")
            result = result.to_i if [:int, :counter].include?(type)
            if type == :counter
              result
            else
              instance_variable_set(instance_variable, result)
            end
          end
        end
      end

      def defined_setter(instance_variable, namespace, property)
        define_method("#{property}=") do |value|
          Bolt.redis do |conn|
            instance_variable_set(instance_variable, value)
            conn.set("#{namespace}:#{name}", value)
          end
        end
      end

    end
  end
end
