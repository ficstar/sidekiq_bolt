module Sidekiq
  module Bolt
    module Scripts
      def self.reset!
        @scripts = nil
      end
    end
  end
end

module Cleanup
  extend RSpec::Core::SharedContext

  after do
    Sidekiq::Bolt::Fetch.instance_variable_set(:@processor_allocator, nil)
    Sidekiq::Bolt::Fetch.local_queue.clear
    Sidekiq::Bolt::Scripts.reset!
  end
end
