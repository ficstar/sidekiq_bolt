module Sidekiq
  module Bolt
    class Fetch

      def self.bulk_requeue(*_)
      end

      def initialize(_)
      end

      def retrieve_work
        work = find_work
        work ? work : sleep(1)
      end

      private

      def find_work
        Resource.all.each do |resource|
          queue, work = resource.allocate(1)
          return UnitOfWork.new(queue, resource.name, work) if work
        end
        nil
      end

    end
  end
end
