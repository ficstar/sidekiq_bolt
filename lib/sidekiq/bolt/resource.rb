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
        run_script(:resource_filter, TYPE_FILTER_SCRIPT, NAMESPACE_KEY, types).map do |name|
          new(name)
        end
      end

      def self.workers_required
        Hash.new(0).tap do |workers_required|
          Resource.all.each do |resource|
            limit = resource.limit.nonzero?
            if limit
              workers_required[resource.type] += limit
            else
              workers_required[resource.type] = 1/0.0
            end
          end
        end
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
        run_script(:resource_size, SIZE_SCRIPT, NAMESPACE_KEY, [name, ''])
      end

      def retrying
        run_script(:resource_retrying, RETRYING_SCRIPT, NAMESPACE_KEY, [name, ''])
      end

      def add_work(queue, work, retrying = false)
        run_script(:resource_add_work, ADD_WORK_SCRIPT, NAMESPACE_KEY, [queue, name, work, retrying])
      end

      def allocate(amount)
        run_script(:resource_allocate, ALLOCATE_SCRIPT, NAMESPACE_KEY, [name, amount, identity, *queues.shuffle])
      end

      def free(queue, work)
        run_script(:resource_free, FREE_SCRIPT, NAMESPACE_KEY, [queue, name, work, identity])
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
