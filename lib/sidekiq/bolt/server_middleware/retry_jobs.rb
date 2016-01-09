module Sidekiq
  module Bolt
    module ServerMiddleware
      class RetryJobs

        def call(worker, job, queue)
          yield
        rescue Exception => e
          raise unless job['retry'] && (!worker.sidekiq_should_retry_block || worker.sidekiq_should_retry_block.call(job, e))

          job['error'] = e
          retry_count_key = "retry_count:#{e}"
          job[retry_count_key] = job[retry_count_key].to_i + 1
          serialized_job = Sidekiq.dump_json(job)
          Resource.new(job['resource']).add_work(job['queue'], serialized_job, true)
        end

      end
    end
  end
end
