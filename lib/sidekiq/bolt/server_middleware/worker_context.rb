module Sidekiq
  module Bolt
    module ServerMiddleware
      class WorkerContext

        def call(worker, job, _, &block)
          successfully_setup = worker.respond_to?(:setup) ? worker.setup : true

          if successfully_setup
            handle_successful_setup(worker, &block)
          else
            handle_erroneous_setup(job, worker)
          end

        ensure
          worker.teardown if worker.respond_to?(:teardown)
        end

        private

        def handle_successful_setup(worker, &block)
          if worker.respond_to?(:context)
            unless yield_worker_context(worker, &block)
              raise "Expected worker '#{worker.class}' #context to yield, but it didn't!"
            end
          else
            yield
          end
        end

        def yield_worker_context(worker)
          valid_context = false
          worker.context do
            valid_context = true
            yield
          end
          valid_context
        end

        def handle_erroneous_setup(job, worker)
          Sidekiq.logger.debug 'Worker skipped due to #setup returning false'

          serialized_job = Sidekiq.dump_json(job)
          worker.resource.add_work(job['queue'], serialized_job)
          job.delete('jid')
        end

      end
    end
  end
end
