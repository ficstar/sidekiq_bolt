module Sidekiq
  module Bolt
    class ProcessorAllocator
      MUTEX = Mutex.new

      def initialize(options)
        @resources = if options[:concurrency]
                       {nil => options[:concurrency]}
                     else
                       options[:concurrency_pool]
                     end
        @allocation = Hash.new(0)
      end

      def allocate(amount, resource_name = nil)
        MUTEX.synchronize do
          concurrency = @resources[resource_name]
          @allocation[resource_name] += amount
          diff = concurrency - @allocation[resource_name]
          if diff < 0
            amount += diff
            @allocation[resource_name] += diff
          end
          amount
        end
      end

    end
  end
end
