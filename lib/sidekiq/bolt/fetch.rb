module Sidekiq
  module Bolt
    class Fetch

      def self.bulk_requeue(*_)
      end

      def initialize(options)
        @supported_resource_types = options.fetch(:resource_types) { :* }
      end

      def retrieve_work
        work = find_work
        work ? work : sleep(1)
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
