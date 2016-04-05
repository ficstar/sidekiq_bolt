require 'rspec'

module Sidekiq
  module Bolt
    describe Bolt do

      describe '.configure_server' do
        before do
          Sidekiq.options[:additional_scheduled_enqs] = ['SomeEnq']
          Bolt.configure_server(Sidekiq)
        end
        after { Sidekiq.options[:additional_scheduled_enqs] = nil }

        it 'should set the fetch to our fetch' do
          expect(Sidekiq.options[:fetch]).to eq(Fetch)
        end

        it 'should set the scheduled enq to our RetryPoller' do
          expect(Sidekiq.options[:scheduled_enq]).to eq(Poller)
        end

        it 'should remove the Sidekiq::Middleware::Server::RetryJobs server middleware' do
          expect(Sidekiq.server_middleware).not_to exist(Middleware::Server::RetryJobs)
        end

        it 'should remove the Sidekiq::Middleware::Server::Logging server middleware' do
          expect(Sidekiq.server_middleware).not_to exist(Middleware::Server::Logging)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::TypeSafety middleware' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::TypeSafety)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::JobSuccession middleware' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::JobSuccession)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::RetryJobs server middleware' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::RetryJobs)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::ResourceInvalidator server middleware' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::ResourceInvalidator)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::WorkerContext server middleware' do
          expect(Sidekiq.server_middleware).to exist(ServerMiddleware::WorkerContext)
        end

        it 'should add the Sidekiq::Bolt::ServerMiddleware::MetaDataMiddleware as the first server middleware' do
          expect(Sidekiq.server_middleware.first.klass).to eq(ServerMiddleware::JobMetaData)
        end

        it 'should include the JobRecoveryEnq among the additional_scheduled_enqs' do
          expect(Sidekiq.options[:additional_scheduled_enqs]).to include(JobRecoveryEnq)
        end

        it 'should include previously defined additional_scheduled_enqs' do
          expect(Sidekiq.options[:additional_scheduled_enqs]).to include('SomeEnq')
        end

        it 'should add the Sidekiq::Bolt::ClientMiddleware::BlockQueue middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::BlockQueue)
        end

        it 'should add the Sidekiq::Bolt::ClientMiddleware::TypeSafety middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::TypeSafety)
        end

        it 'should add the Sidekiq::Bolt::ClientMiddleware::JobSuccession middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::JobSuccession)
        end
      end

      describe '.configure_client' do
        before { Bolt.configure_client(Sidekiq) }

        it 'should add the Sidekiq::Bolt::ClientMiddleware::BlockQueue middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::BlockQueue)
        end

        it 'should add the Sidekiq::Bolt::ClientMiddleware::TypeSafety middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::TypeSafety)
        end

        it 'should add the Sidekiq::Bolt::ClientMiddleware::JobSuccession middleware' do
          expect(Sidekiq.client_middleware).to exist(ClientMiddleware::JobSuccession)
        end
      end

    end
  end
end
