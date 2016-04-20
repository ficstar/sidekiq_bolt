module Sidekiq
  module Bolt
    class WorkFuturePoller
      WAITING = Concurrent::Map.new

      def self.await(job_id)
        observable = Concurrent::IVar.new
        WAITING[job_id] = observable
        ThomasUtils::Observation.new(ThomasUtils::Future::DEFAULT_EXECUTOR, observable)
      end

      def enqueue_jobs
        pending_jobs = WAITING.keys
        serialized_results(pending_jobs).each.with_index do |serialized_result, index|
          job_id = pending_jobs[index]
          resolve_future(job_id, serialized_result)
        end
      end

      private

      def resolve_future(job_id, serialized_result)
        if serialized_result
          result = Sidekiq.load_json(serialized_result)
          observable = WAITING[job_id]
          if result.is_a?(SerializableError)
            observable.fail(result)
          else
            observable.set(result)
          end
          WAITING.delete(job_id)
        end
      end

      def serialized_results(pending_jobs)
        Bolt.redis do |redis|
          redis.multi do
            pending_jobs.each do |job_id|
              redis.get("worker:results:#{job_id}")
            end
          end
        end
      end

    end
  end
end