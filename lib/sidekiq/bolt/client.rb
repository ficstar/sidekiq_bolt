module Sidekiq
  module Bolt
    class Client < Sidekiq::Client

      private

      def atomic_push(_, payloads)
        return super if payloads.first['at']

        now = Time.now
        payloads.each do |entry|
          queue_name = entry['queue']
          queue = Queue.new(queue_name)
          resource_name = entry['resource'] || 'default'
          entry['enqueued_at'.freeze] = now
          work = Sidekiq.dump_json(entry)
          queue.enqueue(resource_name, work)
        end
      end

    end
  end
end
