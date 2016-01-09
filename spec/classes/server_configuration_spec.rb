require 'rspec'

module Sidekiq
  module Bolt
    describe Bolt do

      describe '.configure_server' do
        before { Bolt.configure_server(Sidekiq) }

        it 'should set the fetch to our fetch' do
          expect(Sidekiq.options[:fetch]).to eq(Fetch)
        end

        it 'should remove the Sidekiq::Middleware::Server::RetryJobs' do
          expect(Sidekiq.server_middleware).not_to exist(Middleware::Server::RetryJobs)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::RetryJobs' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::RetryJobs)
        end
      end

    end
  end
end
