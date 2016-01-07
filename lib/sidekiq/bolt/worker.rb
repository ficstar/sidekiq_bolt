module Sidekiq
  module Bolt
    module Worker

      def self.included(base)
        base.send(:include, Sidekiq::Worker)
        base.extend ClassMethods
      end

      module ClassMethods
        def perform_async_with_options(options, *args)
          client_push('class' => self, 'args' => args, 'queue' => options[:queue], 'resource' => options[:resource])
        end

        private
        def client_push(item)
          Sidekiq::Bolt::Client.new.push(item)
        end
      end

    end
  end
end
