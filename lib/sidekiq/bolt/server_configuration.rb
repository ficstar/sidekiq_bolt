module Sidekiq
  module Bolt

    def self.configure_server(config)
      config.options[:fetch] = Fetch
      config.options[:scheduled_enq] = Poller
      config.options[:additional_scheduled_enqs] ||= []
      config.options[:additional_scheduled_enqs] << JobRecoveryEnq
      config.server_middleware do |chain|
        chain.prepend ServerMiddleware::JobMetaData
        chain.add ServerMiddleware::TypeSafety
        chain.add ServerMiddleware::JobSuccession
        chain.remove Middleware::Server::RetryJobs
        chain.add ServerMiddleware::RetryJobs
        chain.add ServerMiddleware::WorkerContext
        chain.add ServerMiddleware::DeferWork
      end
      configure_client(config)
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
