module Sidekiq
  module Bolt
    class EncodedTime

      def initialize(time_in_seconds)
        @time = time_in_seconds
      end

      def to_time
        Time.at(@time)
      end

    end
  end
end
