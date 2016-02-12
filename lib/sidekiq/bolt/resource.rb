module Sidekiq
  module Bolt
    class Resource < Struct.new(:name)
      include Util
      include Scripts
      extend PropertyList

      ASYNC_LOCAL_RESOURCE = '$async_local'

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      TYPE_FILTER_SCRIPT_PATH = "#{SCRIPT_ROOT}/type_filter.lua"
      TYPE_FILTER_SCRIPT = File.read(TYPE_FILTER_SCRIPT_PATH)
      ALLOCATE_SCRIPT_PATH = "#{SCRIPT_ROOT}/alloc.lua"
      ALLOCATE_SCRIPT = File.read(ALLOCATE_SCRIPT_PATH)
      ADD_WORK_SCRIPT_PATH = "#{SCRIPT_ROOT}/add_work.lua"
      ADD_WORK_SCRIPT = File.read(ADD_WORK_SCRIPT_PATH)
      SIZE_SCRIPT_PATH = "#{SCRIPT_ROOT}/size.lua"
      SIZE_SCRIPT = File.read(SIZE_SCRIPT_PATH)
      FREE_SCRIPT_PATH = "#{SCRIPT_ROOT}/free.lua"
      FREE_SCRIPT = File.read(FREE_SCRIPT_PATH)
      RETRYING_SCRIPT_PATH = "#{SCRIPT_ROOT}/retrying.lua"
      RETRYING_SCRIPT = File.read(RETRYING_SCRIPT_PATH)
      NAMESPACE_KEY = [''].freeze

      define_property 'resource:type', :type
      define_property 'resource:limit', :limit, :int

      def self.all
        Bolt.redis { |redis| redis.smembers('resources') }.map { |name| new(name) }
      end

      def self.having_types(types)
        run_script(:resource_filter, TYPE_FILTER_SCRIPT) do |redis, script_sha|
          redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: types)
        end.map { |resource_name| new(resource_name) }
      end

      def frozen=(value)
        Bolt.redis do |redis|
          if value
            redis.set("resource:frozen:#{name}", 1)
          else
            redis.del("resource:frozen:#{name}")
          end
        end
      end

      def frozen
        Bolt.redis { |redis| !!redis.get("resource:frozen:#{name}") }
      end

      def allocated
        Bolt.redis { |redis| redis.get(allocated_key).to_i }
      end

      def over_allocated
        Bolt.redis { |redis| redis.get(over_allocated_key).to_i }
      end

      def queues
        Bolt.redis { |redis| redis.smembers("resource:queues:#{name}") }
      end

      def size
        run_script(:resource_size, SIZE_SCRIPT) do |redis, script_sha|
          redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: [name, ''])
        end
      end

      def retrying
        run_script(:resource_retrying, RETRYING_SCRIPT) do |redis, script_sha|
          redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: [name, ''])
        end
      end

      def add_work(queue, work, retrying = false)
        run_script(:resource_add_work, ADD_WORK_SCRIPT) do |redis, script_sha|
          redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: [queue, name, work, retrying])
        end
      end

      def allocate(amount)
        run_script(:resource_allocate, ALLOCATE_SCRIPT) do |redis, script_sha|
          redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: [name, amount, identity, *queues.shuffle])
        end
      end

      def free(queue, work)
        run_script(:resource_free, FREE_SCRIPT) do |redis, script_sha|
          redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: [queue, name, work, identity])
        end
      end

      private

      def allocated_key
        "resource:allocated:#{name}"
      end

      def over_allocated_key
        "resource:over-allocated:#{name}"
      end

    end
  end
end
