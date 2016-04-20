module Sidekiq
  module Bolt
    module ServerMiddleware
      class Persistence

        def call(_, job, _)
          future = ThomasUtils::Future.immediate { yield }

          (job['persist'] && job['jid']) ? persist_result(future, job) : future
        end

        private

        def persist_result(future, job)
          future.on_complete do |value, error|
            serialized_value = serialized_work_result(error, value)
            Bolt.redis { |redis| redis.set(worker_result_key(job), serialized_value) }
          end
        end

        def worker_result_key(job)
          "worker:results:#{job['jid']}"
        end

        def serialized_work_result(error, value)
          Sidekiq.dump_json(work_result(error, value))
        end

        def work_result(error, value)
          if error
            SerializableError.new(error)
          else
            value
          end
        end

      end
    end
  end
end
