module Sidekiq
  module Bolt
    module ServerMiddleware
      class WorkerContext
        def call(worker, job, _)

          successfully_setup = worker.respond_to?(:setup) ? worker.setup : true

          if successfully_setup
            if worker.respond_to?(:context)
              valid_context = false
              worker.context do
                valid_context = true
                yield
              end
              raise "Expected worker '#{worker.class}' #context to yield, but it didn't!" unless valid_context
            else
              yield
            end
          else
            Sidekiq.logger.debug 'Worker skipped due to #setup returning false'

            serialized_job = Sidekiq.dump_json(job)
            Resource.new(job['resource']).add_work(job['queue'], serialized_job)
            job.delete('jid')
          end

        ensure
          worker.teardown if worker.respond_to?(:teardown)
        end
      end
    end
  end
end
