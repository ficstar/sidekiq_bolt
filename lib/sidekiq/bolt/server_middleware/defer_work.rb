module Sidekiq
  module Bolt
    module ServerMiddleware
      class DeferWork

        def call(worker, job, _)
          if job['defer']
            future = perform_work(worker, job['args'])
            raise 'Expected worker to return a future!' unless future.respond_to?(:on_complete)
            future.on_complete do |_, error|
              worker.acknowledge_work(error)
            end
          else
            yield
          end
        end

        private

        def perform_work(worker, args)
          worker.perform(*cloned_args(args))
        end

        def cloned_args(args)
          Marshal.load(Marshal.dump(args))
        end

      end
    end
  end
end
