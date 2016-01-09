module Sidekiq
  module Bolt
    module ServerMiddleware
      class RetryJobs

        def call(worker, job, queue)
          yield
        rescue Exception => e
          raise unless job['retry']

          job['error'] = e
          serialized_job = Sidekiq.dump_json(job)
          Resource.new(job['resource']).add_work(job['queue'], serialized_job, true)
        end

      end
    end
  end
end
