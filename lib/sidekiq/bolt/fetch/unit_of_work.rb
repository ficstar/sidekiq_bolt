module Sidekiq
  module Bolt
    class Fetch
      UnitOfWork = Struct.new(:queue, :resource_name, :job) do
        alias :queue_name :queue
        alias :message :job

        def acknowledge
          force_acknowledge unless queue == '$async_local'
        end

        def force_acknowledge
          resource.free(queue, job)
        end

        def requeue
          resource.add_work(queue, job)
          acknowledge
        end

        private

        def resource
          @resource ||= Resource.new(resource_name)
        end

      end
    end
  end
end
