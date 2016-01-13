module Sidekiq
  module Bolt
    module ServerMiddleware
      class RetryJobs

        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        ADD_RETRY_SCRIPT_PATH = "#{SCRIPT_ROOT}/add_retry.lua"
        ADD_RETRY_SCRIPT = File.read(ADD_RETRY_SCRIPT_PATH)
        FREEZE_RESOURCE_SCRIPT_PATH = "#{SCRIPT_ROOT}/freeze_resource.lua"
        FREEZE_RESOURCE_SCRIPT = File.read(FREEZE_RESOURCE_SCRIPT_PATH)
        NAMESPACE_KEY = [''].freeze

        def call(worker, job, _)
          yield
        rescue Exception => e
          retry_count_key = "retry_count:#{e}"
          current_retries = job[retry_count_key].to_i
          job[retry_count_key] = current_retries + 1

          if worker.sidekiq_freeze_resource_after_retry_for_block
            unfreeze_in = worker.sidekiq_freeze_resource_after_retry_for_block.call(job, e, job[retry_count_key])
            if unfreeze_in
              desfrost_at = Time.now.to_f + unfreeze_in
              Bolt.redis do |redis|
                redis.eval(FREEZE_RESOURCE_SCRIPT, keys: NAMESPACE_KEY, argv: [job['resource'], desfrost_at])
              end
            end
          end

          unless job['retry'] && (!worker.sidekiq_should_retry_block || worker.sidekiq_should_retry_block.call(job, e, job[retry_count_key]))
            raise
          end

          job['error'] = e
          serialized_job = Sidekiq.dump_json(job)

          if worker.sidekiq_retry_in_block
            retry_in = worker.sidekiq_retry_in_block.call(current_retries, e)
            retry_at = Time.now.to_f + retry_in
            Bolt.redis do |redis|
              redis.eval(ADD_RETRY_SCRIPT, keys: NAMESPACE_KEY, argv: [job['queue'], job['resource'], serialized_job, retry_at])
            end
          else
            Resource.new(job['resource']).add_work(job['queue'], serialized_job, true)
          end
        end

      end
    end
  end
end
