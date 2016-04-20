module Sidekiq
  module Bolt
    class WorkFuturePoller
      WAITING = Concurrent::Map.new

      def self.await(job_id)
        observable = Concurrent::IVar.new
        WAITING[job_id] = observable
        ThomasUtils::Observation.new(ThomasUtils::Future::IMMEDIATE_EXECUTOR, observable)
      end

      def enqueue_jobs
        pending_jobs = WAITING.keys
        serialized_results = Bolt.redis do |redis|
          redis.multi do
            pending_jobs.each do |job_id|
              redis.get("worker:results:#{job_id}")
            end
          end
        end
        serialized_results.each.with_index do |serialized_result, index|
          job_id = pending_jobs[index]
          if serialized_result
            result = Sidekiq.load_json(serialized_result)
            observable = WAITING[job_id]
            observable.set(result)
            WAITING.delete(job_id)
          end
        end
      end

    end
  end
end