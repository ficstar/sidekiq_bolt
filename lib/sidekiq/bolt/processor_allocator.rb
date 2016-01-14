module Sidekiq
  module Bolt
    class ProcessorAllocator
      MUTEX = Mutex.new

      def initialize(options)
        @concurrency = options[:concurrency]
        @allocation = 0
      end

      def allocate(amount)
        MUTEX.synchronize do
          @allocation += amount
          diff = @concurrency - @allocation
          if diff < 0
            amount += diff
            @allocation += diff
          end
          amount
        end
      end

    end
  end
end
