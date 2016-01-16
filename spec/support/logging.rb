module Logging
  extend RSpec::Core::SharedContext

  before do
    allow(Sidekiq.logger).to receive(:debug)
    allow(Sidekiq.logger).to receive(:info)
    allow(Sidekiq.logger).to receive(:warn)
    allow(Sidekiq.logger).to receive(:error)
  end
end
