module Sidekiq
  module Bolt

    def self.configure_server(config)
      config.options[:fetch] = Fetch
      config.options[:scheduled_enq] = Poller
      config.options[:additional_scheduled_enqs] ||= []
      config.options[:additional_scheduled_enqs] << JobRecoveryEnq
      config.options[:additional_scheduled_enqs] << WorkFuturePoller
      config.server_middleware do |chain|
        chain.prepend ServerMiddleware::JobMetaData
        chain.add ServerMiddleware::TypeSafety
        chain.add ServerMiddleware::JobSuccession
        chain.add ServerMiddleware::Persistence
        chain.remove Middleware::Server::RetryJobs
        chain.remove Middleware::Server::Logging
        chain.add ServerMiddleware::RetryJobs
        chain.add ServerMiddleware::ResourceInvalidator
        chain.add ServerMiddleware::WorkerContext
      end
      configure_client(config)
      Feed.new(config.options)
    end

    def self.configure_client(config)
      config.client_middleware do |chain|
        chain.add ClientMiddleware::BlockQueue
        chain.add ClientMiddleware::TypeSafety
        chain.add ClientMiddleware::JobSuccession
      end
    end

  end
end
