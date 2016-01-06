module Sidekiq
  module Bolt
    class Fetch
      UnitOfWork = Struct.new(:queue, :resource, :job) do
        alias :queue_name :queue
      end
    end
  end
end
