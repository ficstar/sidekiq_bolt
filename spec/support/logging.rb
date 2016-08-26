module Logging
  extend RSpec::Core::SharedContext

  let(:global_error_log) { [] }

  before do
    allow(Sidekiq.logger).to receive(:debug)
    allow(Sidekiq.logger).to receive(:info)
    allow(Sidekiq.logger).to receive(:warn)
    allow(Sidekiq.logger).to receive(:error) do |message|
      global_error_log << message
    end
  end
end
