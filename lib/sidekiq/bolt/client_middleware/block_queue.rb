module Sidekiq
  module Bolt
    module ClientMiddleware
      class BlockQueue

        def call(_, msg, _, _ = nil)
          queue_blocked, parent_complete, parent_failed = Bolt.redis do |redis|
            redis.multi do
              redis.get("queue:blocked:#{msg['queue']}")
              redis.get("job_completed:#{msg['pjid']}")
              redis.get("job_failed:#{msg['pjid']}")
            end
          end
          return false if queue_blocked || parent_complete || parent_failed
          yield
        end

      end
    end
  end
end
