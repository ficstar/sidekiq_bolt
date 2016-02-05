module Sidekiq
  module Bolt
    module ServerMiddleware
      class Statistics

        def call(_, job, _)
          yield
          Bolt.redis do |redis|
            redis.pipelined do
              redis.hincrby("resource:stats:#{job['resource']}", 'successful', 1)
              redis.hincrby("queue:stats:#{job['queue']}", 'successful', 1)
            end
          end
        rescue
          Bolt.redis do |redis|
            redis.pipelined do
              redis.hincrby("resource:stats:#{job['resource']}", 'error', 1)
              redis.hincrby("queue:stats:#{job['queue']}", 'error', 1)
            end
          end
          raise
        end

      end
    end
  end
end
