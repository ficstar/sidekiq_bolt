require 'rspec'

module Sidekiq
  module Bolt
    describe Bolt do

      describe '.configure_server' do
        before { Bolt.configure_server(Sidekiq) }

        it 'should set the fetch to our fetch' do
          expect(Sidekiq.options[:fetch]).to eq(Fetch)
        end

        it 'should remove the Sidekiq::Middleware::Server::RetryJobs server middleware' do
          expect(Sidekiq.server_middleware).not_to exist(Middleware::Server::RetryJobs)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::RetryJobs server middleware' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::RetryJobs)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::MetaDataMiddleware as the first server middleware' do
          expect(Sidekiq.server_middleware.first.klass).to eq(ServerMiddleware::JobMetaData)
        end

        it 'should add the Sidekiq::Bolt::ClientMiddleware::BlockQueue client middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::BlockQueue)
        end
      end

      describe '.configure_client' do
        before { Bolt.configure_client(Sidekiq) }

        it 'should add the Sidekiq::Bolt::ClientMiddleware::BlockQueue client middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::BlockQueue)
        end
      end

    end
  end
end
