module TestWork
  extend RSpec::Core::SharedContext

  let(:work_klass) do
    Struct.new(:queue, :resource, :allocation, :work) do
      def self.from_allocations(resource, allocations)
        allocations.each_slice(3).map { |allocation| from_allocation(resource, allocation) }
      end

      def self.from_allocation(resource, allocation)
        queue, allocation, work = allocation
        new(queue, resource, allocation, work)
      end

      alias :message :work
      alias :queue_name :queue

      def acknowledge

      end
    end
  end
end