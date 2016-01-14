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
        @allocation = Hash.new { |hash, key| hash[key] = Allocation.new(Mutex.new, 0) }
      end

      def allocate(amount, resource_name = nil)
        resource_allocation = @allocation[resource_name]
        resource_allocation.mutex.synchronize do
          concurrency = @resources[resource_name]
          resource_allocation.allocation += amount
          diff = concurrency - resource_allocation.allocation
          if diff < 0
            amount += diff
            resource_allocation.allocation += diff
          end
          amount
        end
      end

      def free(amount, resource_name = nil)
        @allocation[resource_name].allocation -= amount
      end

      def allocation(resource_name = nil)
        @allocation[resource_name].allocation
      end

      private

      Allocation = Struct.new(:mutex, :allocation)

    end
  end
end
