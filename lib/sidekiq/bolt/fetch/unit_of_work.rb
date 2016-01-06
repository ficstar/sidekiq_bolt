module Sidekiq
  module Bolt
    class Fetch
      UnitOfWork = Struct.new(:queue, :resource, :job) do
        alias :queue_name :queue

        def acknowledge
          Sidekiq::Bolt::Resource.new(resource).free(queue, job)
        end

      end
    end
  end
end
