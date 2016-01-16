module Sidekiq
  module Bolt
    class Fetch
      MUTEX = Mutex.new

      class << self
        attr_reader :processor_allocator

        def bulk_requeue(*_)
        end

        def processor_allocator=(value)
          MUTEX.synchronize { @processor_allocator ||= value }
        end
      end

      def initialize(options)
        self.class.processor_allocator = ProcessorAllocator.new(options)
        @supported_resource_types = options.fetch(:resource_types) { :* }
      end

      def retrieve_work
        work = find_work
        work ? work : (sleep(1) && nil)
      end

      private

      def find_work
        supported_resources.each do |resource|
          queue, work = resource.allocate(1)
          return UnitOfWork.new(queue, resource.name, work) if work
        end
        nil
      end

      def supported_resources
        (@supported_resource_types == :*) ? Resource.all : Resource.having_types(@supported_resource_types)
      end

    end
  end
end
