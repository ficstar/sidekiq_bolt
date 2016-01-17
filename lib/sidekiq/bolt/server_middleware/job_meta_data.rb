module Sidekiq
  module Bolt
    module ServerMiddleware
      class JobMetaData

        def call(worker, job, _)
          worker.queue = Queue.new(job['queue'])
          worker.resource = Resource.new(job['resource'])
          worker.parent_job_id = job['pjid']
          worker.original_message = job
          yield
        end

      end
    end
  end
end
