module Cleanup
  extend RSpec::Core::SharedContext

  after { Sidekiq::Bolt::Fetch.instance_variable_set(:@processor_allocator, nil) }
end
