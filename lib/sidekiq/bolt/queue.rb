module Sidekiq
  module Bolt
    class Queue < Struct.new(:name)
      include Scripts

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      RETRYING_SCRIPT_PATH = "#{SCRIPT_ROOT}/retrying.lua"
      RETRYING_SCRIPT = File.read(RETRYING_SCRIPT_PATH)
      NAMESPACE_KEY = [''].freeze

      def self.all
        Bolt.redis do |conn|
          conn.smembers('queues')
        end.map { |name| new(name) }
      end

      def self.enqueue(list_of_items)
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
        Bolt.redis { |redis| redis.hget("queue:stats:#{name}", 'error') }.to_i
      end

      def busy
        Bolt.redis { |redis| redis.get("queue:busy:#{name}").to_i }
      end

      def enqueue(resource, workload, retrying = false)
        resource = Resource.new(resource) unless resource.is_a?(Resource)
        resource.add_work(name, workload, retrying)
      end

    end
  end
end
