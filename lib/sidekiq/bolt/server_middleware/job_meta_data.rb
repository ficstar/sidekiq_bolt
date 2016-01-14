module Sidekiq
  module Bolt
    module ServerMiddleware
      class JobMetaData

        def call(worker, job, _)
          worker.queue = Queue.new(job['queue'])
          worker.resource = Resource.new(job['resource'])
          worker.parent_job_id = job['pjid']
          yield
        end

      end
    end
  end
end
