module Sidekiq
  module Bolt
    module Worker
      attr_accessor :queue, :resource

      def self.included(base)
        base.send(:include, Sidekiq::Worker)
        base.extend ClassMethods
        base.class_attribute :sidekiq_should_retry_block
      end

      module ClassMethods
        def perform_async_with_options(options, *args)
          client_push('class' => self, 'args' => args, 'queue' => options[:queue], 'resource' => options[:resource])
        end

        def sidekiq_should_retry?(&block)
          self.sidekiq_should_retry_block = block
        end

        private
        def client_push(item)
          Sidekiq::Bolt::Client.new.push(item)
        end
      end

    end
  end
end
