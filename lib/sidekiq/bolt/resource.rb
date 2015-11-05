module Sidekiq
  module Bolt
    class Resource < Struct.new(:name)
      extend PropertyList

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      ALLOCATE_SCRIPT_PATH = "#{SCRIPT_ROOT}/alloc.lua"
      ALLOCATE_SCRIPT = File.read(ALLOCATE_SCRIPT_PATH)

      define_property 'resource:type', :type
      define_property 'resource:limit', :limit, :int

      def allocated
        Bolt.redis { |redis| redis.get(allocated_key).to_i }
      end

      def add_work(queue, work)
        Bolt.redis do |redis|
          redis.lpush("resource:queue:#{queue}:#{name}", work)
        end
      end

      def allocate(amount)
        Bolt.redis do |redis|
          redis.eval(ALLOCATE_SCRIPT, keys: [''], argv: [name, amount])
        end
      end

      def free
        Bolt.redis { |redis| redis.decr(allocated_key) }
      end

      private

      def allocated_key
        "resource:allocated:#{name}"
      end

    end
  end
end
