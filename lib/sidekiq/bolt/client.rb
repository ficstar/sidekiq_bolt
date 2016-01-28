module Sidekiq
  module Bolt
    class Client < Sidekiq::Client

      NAMESPACE_KEY = [''].freeze
      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      BACKUP_WORK_SCRIPT_PATH = "#{SCRIPT_ROOT}/backup.lua"
      BACKUP_WORK_DEPENDENCY_SCRIPT = File.read(BACKUP_WORK_SCRIPT_PATH)

      def skeleton_push(item)
        queue_name = item['queue']
        work = Sidekiq.dump_json(item)
        if item['resource'] == Resource::ASYNC_LOCAL_RESOURCE
          backup_work(item, work)
          run_work_now(item, queue_name)
        elsif allocate_worker(item)
          unless backup_work(item, work)
            enqueue_item(item, work)
            Fetch.processor_allocator.free(1, item['resource'])
          end
        else
          enqueue_item(item, work)
        end
      end

      def allocate_worker(item)
        Fetch.processor_allocator.allocate(1, item['resource']).nonzero?
      end

      private

      def raw_push(payloads)
        @redis_pool.with do |conn|
          atomic_push(conn, payloads)
        end
        true
      end

      def atomic_push(_, payloads)
        if payloads.first['at']
          payloads.each { |item| item['sk'] = 'bolt' }
          return super
        end

        now = Time.now.to_f
        payloads.each do |entry|
          entry['resource'] ||= 'default'
          entry['enqueued_at'.freeze] = now
          skeleton_push(entry)
        end
      end

      def backup_work(item, work)
        argv = [item['queue'], item['resource'], work, Socket.gethostname]
        Bolt.redis do |redis|
          redis.eval(BACKUP_WORK_DEPENDENCY_SCRIPT, keys: NAMESPACE_KEY, argv: argv)
        end
      end

      def run_work_now(item, queue_name)
        worker = item['class'].constantize.new
        Sidekiq.server_middleware.invoke(worker, item, queue_name) do
          worker.perform(*item['args'])
        end
      end

      def enqueue_item(item, work)
        queue = Queue.new(item['queue'])
        queue.enqueue(item['resource'], work, !!item['error'])
      end

    end
  end

  class Client
    def self.push(item)
      (item['sk'] == 'bolt' ? Bolt::Client : self).new.push(item)
    end
  end

end
