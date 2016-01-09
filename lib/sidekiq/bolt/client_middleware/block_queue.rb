module Sidekiq
  module Bolt
    module ClientMiddleware
      class BlockQueue

        def call(_, msg, _, _ = nil)
          queue = Queue.new(msg['queue'])
          return false if queue.blocked
          yield
        end

      end
    end
  end
end
