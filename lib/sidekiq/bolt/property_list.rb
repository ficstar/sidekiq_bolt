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
        define_incr(namespace, property)
        define_incrby(namespace, property)
        define_decr(namespace, property)
        define_decrby(namespace, property)
      end

      def define_decrby(namespace, property)
        define_method("#{property}_decrby") do |amount|
          Bolt.redis do |conn|
            conn.decrby("#{namespace}:#{name}", amount)
          end
        end
      end

      def define_decr(namespace, property)
        define_method("#{property}_decr") do
          Bolt.redis do |conn|
            conn.decr("#{namespace}:#{name}")
          end
        end
      end

      def define_incrby(namespace, property)
        define_method("#{property}_incrby") do |amount|
          Bolt.redis do |conn|
            conn.incrby("#{namespace}:#{name}", amount)
          end
        end
      end

      def define_incr(namespace, property)
        define_method("#{property}_incr") do
          Bolt.redis do |conn|
            conn.incr("#{namespace}:#{name}")
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
            instance_variable_set(instance_variable, result)
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
