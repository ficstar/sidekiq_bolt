module Setup
  extend RSpec::Core::SharedContext

  let(:sidekiq_options) { {concurrency: 0} }

  before { Sidekiq.options = sidekiq_options }
end
