module Sidekiq
  module Bolt
    module ServerMiddleware
      class WorkerContext
        def call(worker, _, _)
          worker.setup if worker.respond_to?(:setup)
          if worker.respond_to?(:context)
            valid_context = false
            worker.context do
              valid_context = true
              yield
            end
            raise "Expected worker '#{worker.class}' #context to yield, but it didn't!" unless valid_context
          else
            yield
          end
          worker.teardown if worker.respond_to?(:teardown)
        end
      end
    end
  end
end
