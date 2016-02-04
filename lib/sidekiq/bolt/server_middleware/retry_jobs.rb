module Sidekiq
  module Bolt
    module ServerMiddleware
      class RetryJobs

        Retry = Struct.new(:job, :error, :error_retries, :total_retries, :resource_retries, :queue_retries)

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
          handle_retry(error, job, worker)
          job.delete('jid')
        end

        private

        def handle_retry(error, job, worker)
          increment_retry_counts(error, job)

          resource = Resource.new(job['resource'])
          job_retry = job_retry(error, job)

          freeze_resource_if_necessary!(job, job_retry, resource, worker)

          raise unless can_retry?(job, job_retry, worker)

          Sidekiq.logger.warn("Retrying job '#{job['jid']}': #{error}\n#{error.backtrace * "\n"}")

          job['error'] = error
          serialized_job = Sidekiq.dump_json(job)

          retry_job!(job, job_retry, resource, serialized_job, worker)
        end

        def increment_retry_counts(error, job)
          job['retry_count:total'] = job['retry_count:total'].to_i + 1

          retry_count_key = "retry_count:#{error}"
          job[retry_count_key] = job[retry_count_key].to_i + 1
        end

        def job_retry(error, job)
          resource_retries, queue_retries = Bolt.redis do |redis|
            redis.multi do
              redis.incr("resource:retries:#{job['resource']}")
              redis.incr("queue:retries:#{job['queue']}")
            end
          end

          Retry.new(job, error, job["retry_count:#{error}"], job['retry_count:total'], resource_retries, queue_retries)
        end

        def retry_job!(job, job_retry, resource, serialized_job, worker)
          if worker.sidekiq_retry_in_block
            schedule_retry(job, job_retry, serialized_job, worker)
          else
            retry_job_now!(job, resource, serialized_job)
          end
        end

        def retry_job_now!(job, resource, serialized_job)
          resource.add_work(job['queue'], serialized_job, true)
        end

        def schedule_retry(job, job_retry, serialized_job, worker)
          retry_at = retry_job_at(job_retry, worker)
          Bolt.redis do |redis|
            redis.eval(ADD_RETRY_SCRIPT, keys: NAMESPACE_KEY, argv: [job['queue'], job['resource'], serialized_job, retry_at])
          end
        end

        def retry_job_at(job_retry, worker)
          retry_in = worker.sidekiq_retry_in_block.call(job_retry)
          Time.now.to_f + retry_in
        end

        def can_retry?(job, job_retry, worker)
          job['retry'] && (job['retry'] == true || job['retry_count:total'] <= job['retry'].to_i) &&
              (!worker.sidekiq_should_retry_block || worker.sidekiq_should_retry_block.call(job_retry))
        end

        def freeze_resource_if_necessary!(job, job_retry, resource, worker)
          if worker.sidekiq_freeze_resource_after_retry_for_block
            unfreeze_in = worker.sidekiq_freeze_resource_after_retry_for_block.call(job_retry)
            freeze_resource!(job, resource, unfreeze_in) if unfreeze_in
          end
        end

        def freeze_resource!(job, resource, unfreeze_in)
          if unfreeze_in == :forever
            resource.frozen = true
          else
            schedule_resource_defrost!(job, unfreeze_in)
          end
        end

        def schedule_resource_defrost!(job, unfreeze_in)
          desfrost_at = Time.now.to_f + unfreeze_in
          Bolt.redis do |redis|
            redis.eval(FREEZE_RESOURCE_SCRIPT, keys: NAMESPACE_KEY, argv: [job['resource'], desfrost_at])
          end
        end

      end
    end
  end
end
