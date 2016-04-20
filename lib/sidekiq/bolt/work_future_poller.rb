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
        WAITING.keys.each do |job_id|
          serialized_result = Bolt.redis { |redis| redis.get("worker:results:#{job_id}") }
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