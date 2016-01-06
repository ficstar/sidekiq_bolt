module Sidekiq
  module Bolt
    class Fetch
      UnitOfWork = Struct.new(:queue, :resource, :job) do
        alias :queue_name :queue

        def acknowledge
          Sidekiq::Bolt::Resource.new(resource).free(queue, job)
        end

        def requeue
          Sidekiq::Bolt::Resource.new(resource).add_work(queue, job)
          acknowledge
        end

      end
    end
  end
end
