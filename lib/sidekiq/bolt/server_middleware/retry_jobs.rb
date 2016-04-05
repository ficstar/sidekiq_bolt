module Sidekiq
  module Bolt
    module ServerMiddleware
      class RetryJobs
        include Scripts

        RETRY_PROPERTIES = [
            :job,
            :error,
            :error_retries,
            :total_retries,
            :resource_retries,
            :resource_error_retries,
            :queue_retries,
            :queue_error_retries
        ]
        Retry = Struct.new(*RETRY_PROPERTIES)

        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        ADD_RETRY_SCRIPT_PATH = "#{SCRIPT_ROOT}/add_retry.lua"
        ADD_RETRY_SCRIPT = File.read(ADD_RETRY_SCRIPT_PATH)
        FREEZE_RESOURCE_SCRIPT_PATH = "#{SCRIPT_ROOT}/freeze_resource.lua"
        FREEZE_RESOURCE_SCRIPT = File.read(FREEZE_RESOURCE_SCRIPT_PATH)
        NAMESPACE_KEY = [''].freeze

        def call(worker, job, _)
          ThomasUtils::Future.immediate { yield }.fallback do |error|
            handle_retry(error, job, worker)
            job.delete('jid')
          end
        end

        private

        def handle_retry(error, job, worker)
          increment_retry_counts(error, job)

          resource = worker.resource
          job_retry = job_retry(error, job)

          freeze_resource_if_necessary!(job, job_retry, resource, worker)

          raise error unless can_retry?(job, job_retry, worker)

          Sidekiq.logger.warn("Retrying job '#{job['jid']}': #{error}\n#{error.backtrace * "\n"}")

          job['error'] = error
          serialized_job = Sidekiq.dump_json(job)

          retry_job!(job, job_retry, resource, serialized_job, worker)
        end

        def increment_retry_counts(error, job)
          retry_counts = (job['retry_count'] ||= {})
          retry_counts['total'] = retry_counts['total'].to_i + 1

          retry_count_key = error.to_s
          retry_counts[retry_count_key] = retry_counts[retry_count_key].to_i + 1
        end

        def job_retry(error, job)
          resource_retries, resource_error_retries, queue_retries, queue_error_retries = Bolt.redis do |redis|
            redis.multi do
              redis.hincrby("resource:retry_count:#{job['resource']}", 'total', 1)
              redis.hincrby("resource:retry_count:#{job['resource']}", error.to_s, 1)
              redis.hincrby("queue:retry_count:#{job['queue']}", 'total', 1)
              redis.hincrby("queue:retry_count:#{job['queue']}", error.to_s, 1)
            end
          end

          retry_counts = job['retry_count']
          Retry.new(job, error, retry_counts[error.to_s], retry_counts['total'], resource_retries, resource_error_retries, queue_retries, queue_error_retries)
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
          run_script(:retry_jobs_add_retry, ADD_RETRY_SCRIPT, NAMESPACE_KEY, [job['queue'], job['resource'], serialized_job, retry_at])
        end

        def retry_job_at(job_retry, worker)
          retry_in = worker.sidekiq_retry_in_block.call(job_retry)
          Time.now.to_f + retry_in
        end

        def can_retry?(job, job_retry, worker)
          job['retry'] && (job['retry'] == true || job['retry_count']['total'].to_i <= job['retry'].to_i) &&
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
          run_script(:retry_jobs_freeze_resource, FREEZE_RESOURCE_SCRIPT, NAMESPACE_KEY, [job['resource'], desfrost_at])
        end

      end
    end
  end
end
