module Sidekiq
  module Bolt
    def self.configure_server(config)
      config.options[:fetch] = Fetch
      config.server_middleware do |chain|
        chain.prepend ServerMiddleware::JobMetaData
        chain.remove Middleware::Server::RetryJobs
        chain.add ServerMiddleware::RetryJobs
      end
    end
  end
end
