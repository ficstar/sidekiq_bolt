module Sidekiq
  module Bolt
    class Queue < Struct.new(:name)
      include Scripts
      extend PropertyList

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      RETRYING_SCRIPT_PATH = "#{SCRIPT_ROOT}/retrying.lua"
      RETRYING_SCRIPT = File.read(RETRYING_SCRIPT_PATH)
      FILTER_GROUPED_QUEUES_SCRIPT_PATH = "#{SCRIPT_ROOT}/filter.lua"
      FILTER_GROUPED_QUEUES_SCRIPT = File.read(FILTER_GROUPED_QUEUES_SCRIPT_PATH)
      NAMESPACE_KEY = [''].freeze

      define_property 'queue:group', :group

      class << self
        def all(group = :*)
          grouped_all(group).map { |name| new(name) }
        end

        def enqueue(list_of_items)
          script_sha = Scripts.load_script(:resource_add_work, Resource::ADD_WORK_SCRIPT)
          Bolt.redis do |redis|
            redis.pipelined do
              list_of_items.each do |item|
                argv = [item[:queue], item[:resource], item[:work]]
                redis.evalsha(script_sha, keys: NAMESPACE_KEY, argv: argv)
              end
            end
          end
        end

        private

        def grouped_all(group)
          if group == :*
            Bolt.redis { |conn| conn.smembers('queues') }
          else
            run_script(:grouped_queues, FILTER_GROUPED_QUEUES_SCRIPT, NAMESPACE_KEY, [group])
          end
        end
      end

      def resources
        Bolt.redis do |conn|
          conn.smembers("queue:resources:#{name}")
        end.map { |name| Resource.new(name) }
      end

      def job
        job_name = Bolt.redis { |redis| redis.get("queue:job:#{name}") }
        Job.new(job_name) if job_name
      end

      def paused=(value)
        Bolt.redis do |redis|
          if value
            redis.set("queue:paused:#{name}", 1)
          else
            redis.del("queue:paused:#{name}")
          end
        end
      end

      def paused
        Bolt.redis { |redis| !!redis.get("queue:paused:#{name}") }
      end

      def blocked=(value)
        Bolt.redis do |redis|
          if value
            redis.set("queue:blocked:#{name}", 1)
          else
            redis.del("queue:blocked:#{name}")
          end
        end
      end

      def blocked
        Bolt.redis { |redis| !!redis.get("queue:blocked:#{name}") }
      end

      def size
        resources.map { |resource| resource.size_for_queue(name) }.reduce(&:+) || 0
      end

      def retrying
        run_script(:queue_retrying, RETRYING_SCRIPT, NAMESPACE_KEY, [name, ''])
      end

      def error_count
        stat_count('error')
      end

      def success_count
        stat_count('successful')
      end

      def busy
        Bolt.redis { |redis| redis.get("queue:busy:#{name}").to_i }
      end

      def enqueue(resource, workload, retrying = false)
        resource = Resource.new(resource) unless resource.is_a?(Resource)
        resource.add_work(name, workload, retrying)
      end

      private

      def stat_count(stat)
        Bolt.redis { |redis| redis.hget("queue:stats:#{name}", stat) }.to_i
      end

    end
  end
end
