require 'rspec'

module Sidekiq
  module Bolt
    describe Bolt do

      describe '.configure_server' do
        before { Bolt.configure_server(Sidekiq) }

        it 'should set the fetch to our fetch' do
          expect(Sidekiq.options[:fetch]).to eq(Fetch)
        end

        it 'should remove the Middleware::Server::RetryJobs' do
          expect(Sidekiq.server_middleware).not_to exist(Middleware::Server::RetryJobs)
        end
      end

    end
  end
end
