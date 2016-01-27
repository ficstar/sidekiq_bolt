module Sidekiq
  module Bolt
    class ProcessorAllocator
      MUTEX = Mutex.new

      def initialize(options)
        @options = options
        @resources = if @options[:concurrency]
                       {nil => @options[:concurrency]}
                     else
                       @options[:concurrency_pool]
                     end
        @allocation = Hash.new { |hash, key| hash[key] = Allocation.new(Mutex.new, 0) }
      end

      def allocate(amount, resource_type = nil)
        return amount if resource_type == '$async_local'

        resource_allocation = @allocation[resource_type]
        resource_allocation.mutex.synchronize do
          concurrency = @resources[resource_type]
          resource_allocation.allocation += amount
          diff = concurrency - resource_allocation.allocation
          if diff < 0
            amount += diff
            resource_allocation.allocation += diff
          end
          amount
        end
      end

      def free(amount, resource_type = nil)
        @allocation[resource_type].allocation -= amount unless resource_type == '$async_local'
      end

      def allocation(resource_type = nil)
        @allocation[resource_type].allocation
      end

      private

      Allocation = Struct.new(:mutex, :allocation)

    end
  end
end
