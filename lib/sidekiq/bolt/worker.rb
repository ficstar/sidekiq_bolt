module Sidekiq
  module Bolt
    module Worker

      def self.included(base)
        base.send(:include, Sidekiq::Worker)
        base.extend ClassMethods
      end

      module ClassMethods
        private
        def client_push(item)
          Sidekiq::Bolt::Client.new.push(item)
        end
      end

    end
  end
end
