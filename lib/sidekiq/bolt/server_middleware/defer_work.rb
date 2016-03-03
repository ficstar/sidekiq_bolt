module Sidekiq
  module Bolt
    module ServerMiddleware
      class DeferWork

        def call(worker, job, _)
          if job['defer']
            future = worker.perform(*Marshal.load(Marshal.dump(job['args'])))
            raise 'Expected worker to return a future!' unless future.respond_to?(:on_complete)
            future.on_complete do |_, error|
              worker.acknowledge_work(error)
            end
          else
            yield
          end
        end

      end
    end
  end
end
