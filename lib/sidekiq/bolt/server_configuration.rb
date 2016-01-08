module Sidekiq
  module Bolt
    def self.configure_server(config)
      config.options[:fetch] = Fetch
      config.server_middleware do |chain|
        chain.remove Sidekiq::Middleware::Server::RetryJobs
      end
    end
  end
end
