module Sidekiq
  module Bolt
    class ProcessorAllocator
      MUTEX = Mutex.new

      def initialize(options)
        @options = options
        max_concurrency = @options[:max_concurrency] || @options[:concurrency]
        @resources = if @options[:concurrency_pool]
                       default_resources_consumed = @options[:concurrency_pool].values.reduce(&:+).to_i
                       default_resources_left = max_concurrency - default_resources_consumed
                       @options[:concurrency_pool].merge(nil => default_resources_left)
                     else
                       {nil => max_concurrency}
                     end
        @allocation = Hash.new { |hash, key| hash[key] = Allocation.new(Mutex.new, 0) }
      end

      def allocate(amount, resource_type = nil)
        return amount if resource_type == Resource::ASYNC_LOCAL_RESOURCE

        resource_allocation = @allocation[resource_type]
        resource_allocation.mutex.synchronize do
          concurrency = @resources[resource_type].to_i
          diff = concurrency - (resource_allocation.allocation + amount)
          amount = amount + diff if diff < 0
          if amount > 0 && (!block_given? || yield)
            resource_allocation.allocation += amount
            amount
          else
            0
          end
        end
      end

      def free(amount, resource_type = nil)
        @allocation[resource_type].allocation -= amount unless resource_type == Resource::ASYNC_LOCAL_RESOURCE
      end

      def allocation(resource_type = nil)
        @allocation[resource_type].allocation
      end

      private

      Allocation = Struct.new(:mutex, :allocation)

    end
  end
end
