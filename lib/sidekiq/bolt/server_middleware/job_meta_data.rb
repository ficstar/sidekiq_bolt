module Sidekiq
  module Bolt
    module ServerMiddleware
      class JobMetaData

        def call(worker, job, _)
          worker.queue = Queue.new(job['queue'])
          worker.resource = Resource.new(job['resource'])
          worker.jid = job['jid']
          worker.parent_job_id = job['pjid']
          worker.original_message = Sidekiq.dump_json(job)
          yield
        end

      end
    end
  end
end
