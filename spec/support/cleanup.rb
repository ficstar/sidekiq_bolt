module Cleanup
  extend RSpec::Core::SharedContext

  after do
    Sidekiq::Bolt::Fetch.instance_variable_set(:@processor_allocator, nil)
    Sidekiq::Bolt::Fetch.local_queue.clear
  end
end
