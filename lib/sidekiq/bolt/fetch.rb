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

        def local_queue
          return @local_queue if @local_queue
          MUTEX.synchronize { @local_queue ||= ::Queue.new }
        end
      end

      def initialize(options)
        @options = options
        processor_allocator
        @supported_resource_types = options.fetch(:resource_types) { :* }
      end

      def retrieve_work
        work = find_work
        work ? work : (sleep(1) && nil)
      end

      private

      attr_reader :options

      def processor_allocator
        self.class.processor_allocator ||= ProcessorAllocator.new(options)
      end

      def find_work

        supported_resources.each do |resource|
          if processor_allocator.allocate(1, resource.type).nonzero?
            queue, work = resource.allocate(1)
            if work
              return UnitOfWork.new(queue, resource.name, work)
            else
              processor_allocator.free(1, resource.type)
            end
          end
        end
        nil
      end

      def supported_resources
        (@supported_resource_types == :*) ? Resource.all : Resource.having_types(@supported_resource_types)
      end

    end
  end
end
