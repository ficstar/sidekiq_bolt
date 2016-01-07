module Sidekiq
  module Bolt
    class Fetch

      def self.bulk_requeue(*_)
      end

      def initialize(_)
      end

      def retrieve_work
        sleep 1
      end

    end
  end
end
