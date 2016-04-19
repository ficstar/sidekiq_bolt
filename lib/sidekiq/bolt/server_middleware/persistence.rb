module Sidekiq
  module Bolt
    module ServerMiddleware
      class Persistence

        def call(_, job, _)
          future = ThomasUtils::Future.immediate do
            yield
          end
          if job['persist']
            future.on_complete do |value, error|
              result = if error
                         SerializableError.new(error)
                       else
                         value
                       end
              serialized_value = Sidekiq.dump_json(result)
              key = "worker:results:#{job['jid']}"
              Bolt.redis { |redis| redis.set(key, serialized_value) }
            end
          else
            future
          end
        end

      end
    end
  end
end