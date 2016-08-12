module Sidekiq
  module Bolt
    class Fetch
      MUTEX = Mutex.new

      class << self
        def bulk_requeue(*_)
        end

        def processor_allocator
          return @processor_allocator if @processor_allocator
          MUTEX.synchronize { @processor_allocator ||= ProcessorAllocator.new(Sidekiq.options) }
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
        self.class.processor_allocator
      end

      def local_queue
        self.class.local_queue
      end

      def find_work
        find_local_work || find_resource_work
      end

      def find_local_work
        local_queue.pop unless local_queue.empty?
      end

      def find_resource_work
        supported_resources.each do |resource|
          workers_available = allocate_worker(resource)
          if workers_available
            work, workers_left = allocate_work(resource, workers_available)
            release_worker(resource, workers_left) if workers_left.nonzero?
            return work
          end
        end
        nil
      end

      def release_worker(resource, wokers_available)
        processor_allocator.free(wokers_available, resource.type)
      end

      def allocate_work(resource, wokers_available)
        items = resource.allocate(wokers_available).each_slice(2).map do |(queue, work)|
          UnitOfWork.new(queue, resource.name, work) if work
        end
        wokers_available -= items.count

        result = items.shift
        items.each { |unit_of_work| local_queue << unit_of_work }
        [result, wokers_available]
      end

      def allocate_worker(resource)
        processor_allocator.allocate(1000, resource.type).nonzero?
      end

      def supported_resources
        (@supported_resource_types == :*) ? Resource.all : Resource.having_types(@supported_resource_types)
      end

    end
  end
end
