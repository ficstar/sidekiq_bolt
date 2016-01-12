module Sidekiq
  module Bolt

    def self.configure_server(config)
      config.options[:fetch] = Fetch
      config.options[:scheduled_enq] = Poller
      config.server_middleware do |chain|
        chain.prepend ServerMiddleware::JobMetaData
        chain.remove Middleware::Server::RetryJobs
        chain.add ServerMiddleware::RetryJobs
      end
      config.client_middleware do |chain|
        chain.add ClientMiddleware::BlockQueue
      end
    end

    def self.configure_client(config)
      config.client_middleware do |chain|
        chain.add ClientMiddleware::BlockQueue
      end
    end

  end
end
