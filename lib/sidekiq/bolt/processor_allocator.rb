module Sidekiq
  module Bolt
    class ProcessorAllocator
      MUTEX = Mutex.new

      def initialize(options)
        @options = options
        @resources = if @options[:concurrency_pool]
                       default_resources_consumed = @options[:concurrency_pool].values.reduce(&:+).to_i
                       default_resources_left = @options[:concurrency] - default_resources_consumed
                       @options[:concurrency_pool].merge(nil => default_resources_left)
                     else
                       {nil => @options[:concurrency]}
                     end
        @allocation = Hash.new { |hash, key| hash[key] = Allocation.new(Mutex.new, 0) }
      end

      def allocate(amount, resource_type = nil)
        return amount if resource_type == Resource::ASYNC_LOCAL_RESOURCE

        resource_allocation = @allocation[resource_type]
        resource_allocation.mutex.synchronize do
          if !block_given? || yield
            concurrency = @resources[resource_type].to_i
            resource_allocation.allocation += amount
            diff = concurrency - resource_allocation.allocation
            if diff < 0 &&
                amount += diff
              resource_allocation.allocation += diff
            end
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
