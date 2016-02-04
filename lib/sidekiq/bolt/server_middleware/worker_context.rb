module Sidekiq
  module Bolt
    module ServerMiddleware
      class WorkerContext
        def call(worker, job, _)
          worker.setup if worker.respond_to?(:setup)
          yield
          worker.teardown if worker.respond_to?(:teardown)
        end
      end
    end
  end
end
