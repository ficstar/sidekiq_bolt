module Sidekiq
  module Bolt
    class Client < Sidekiq::Client

      private

      def atomic_push(_, payloads)
        if payloads.first['at']
          payloads.each { |item| item['sk'] = 'bolt' }
          return super
        end

        now = Time.now
        payloads.each do |entry|
          queue_name = entry['queue']
          queue = Queue.new(queue_name)
          entry['resource'] ||= 'default'
          resource_name = entry['resource']
          entry['enqueued_at'.freeze] = now
          work = Sidekiq.dump_json(entry)
          queue.enqueue(resource_name, work)
        end
      end

    end
  end
end
