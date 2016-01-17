module Sidekiq
  module Bolt
    class Client < Sidekiq::Client

      def skeleton_push(item)
        queue_name = item['queue']
        queue = Queue.new(queue_name)
        resource_name = item['resource']
        work = Sidekiq.dump_json(item)
        queue.enqueue(resource_name, work, !!item['error'])
      end

      private

      def atomic_push(_, payloads)
        if payloads.first['at']
          payloads.each { |item| item['sk'] = 'bolt' }
          return super
        end

        now = Time.now
        payloads.each do |entry|
          entry['resource'] ||= 'default'
          entry['enqueued_at'.freeze] = now
          skeleton_push(entry)
        end
      end

    end
  end

  class Client
    def self.push(item)
      (item['sk'] == 'bolt' ? Bolt::Client : self).new.push(item)
    end
  end

end
