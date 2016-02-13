module Sidekiq
  module Bolt
    class ChildScheduler < Scheduler

      def perform_after_with_options(options, worker_class, *args, &block)
        super(options.merge(parent_job_id: prev_job_id), worker_class, *args, &block)
      end

      def schedule!
        items_to_schedule = items.each_slice(3).map do |(queue, resource, work)|
          {queue: queue, resource: resource, work: work}
        end
        Queue.enqueue(items_to_schedule)
      end

    end
  end
end