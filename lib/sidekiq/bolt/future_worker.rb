module Sidekiq
  module Bolt
    module FutureWorker

      def self.included(base)
        base.send(:include, Worker)
      end

      def perform(*args)
        perform_future(*args).on_complete do |_, error|
          acknowledge_work(error)
        end
      end

    end
  end
end
