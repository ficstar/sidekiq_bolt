module Setup
  extend RSpec::Core::SharedContext

  let(:sidekiq_options) { {concurrency: 0} }

  before do
    Sidekiq.logger.level = Logger::WARN
    Sidekiq.options = sidekiq_options
    allow(ThomasUtils::Future).to receive(:new).and_yield
  end
end
