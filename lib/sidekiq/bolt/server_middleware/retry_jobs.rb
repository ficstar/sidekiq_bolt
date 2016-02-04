module Sidekiq
  module Bolt
    module ServerMiddleware
      class RetryJobs

        Retry = Struct.new(:job, :error, :error_retries, :total_retries)

        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        ADD_RETRY_SCRIPT_PATH = "#{SCRIPT_ROOT}/add_retry.lua"
        ADD_RETRY_SCRIPT = File.read(ADD_RETRY_SCRIPT_PATH)
        FREEZE_RESOURCE_SCRIPT_PATH = "#{SCRIPT_ROOT}/freeze_resource.lua"
        FREEZE_RESOURCE_SCRIPT = File.read(FREEZE_RESOURCE_SCRIPT_PATH)
        NAMESPACE_KEY = [''].freeze

        def call(worker, job, _)
          yield
        rescue Exception => error
          job['retry_count:total'] = job['retry_count:total'].to_i + 1

          retry_count_key = "retry_count:#{error}"
          current_retries = job[retry_count_key].to_i
          job[retry_count_key] = current_retries + 1

          resource = Resource.new(job['resource'])
          job_retry = Retry.new(job, error, job[retry_count_key], job['retry_count:total'])

          if worker.sidekiq_freeze_resource_after_retry_for_block
            unfreeze_in = worker.sidekiq_freeze_resource_after_retry_for_block.call(job_retry)
            if unfreeze_in
              if unfreeze_in == :forever
                resource.frozen = true
              else
                desfrost_at = Time.now.to_f + unfreeze_in
                Bolt.redis do |redis|
                  redis.eval(FREEZE_RESOURCE_SCRIPT, keys: NAMESPACE_KEY, argv: [job['resource'], desfrost_at])
                end
              end
            end
          end

          unless job['retry'] && (!worker.sidekiq_should_retry_block || worker.sidekiq_should_retry_block.call(job_retry))
            raise
          end

          Sidekiq.logger.warn("Retrying job '#{job['jid']}': #{error}\n#{error.backtrace * "\n"}")

          job['error'] = error
          serialized_job = Sidekiq.dump_json(job)

          if worker.sidekiq_retry_in_block
            retry_in = worker.sidekiq_retry_in_block.call(job_retry)
            retry_at = Time.now.to_f + retry_in
            Bolt.redis do |redis|
              redis.eval(ADD_RETRY_SCRIPT, keys: NAMESPACE_KEY, argv: [job['queue'], job['resource'], serialized_job, retry_at])
            end
          else
            resource.add_work(job['queue'], serialized_job, true)
          end
          job.delete('jid')
        end

      end
    end
  end
end
