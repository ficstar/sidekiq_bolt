module Sidekiq
  class Processor

    def process(work)
      @boss.async.real_thread(proxy_id, Thread.current)
      msg = Sidekiq.load_json(work.message)
      worker_klass = msg['class'].constantize
      worker = worker_klass.new
      ack = false
      future = ThomasUtils::Future.none.then do
        Sidekiq.server_middleware.invoke(worker, msg, work.queue_name) do
          ack = true
          worker.resource_allocation = work.allocation
          ThomasUtils::Future.successive(executor: worker_executor(work)) do
            execute_job(worker, cloned(msg['args']))
          end
        end
      end.fallback do |error|
        handle_exception(error, msg)
        ThomasUtils::Future.error(error)
      end.ensure do
        work.acknowledge if ack
      end
      @boss.async.processor_done(current_actor)
      future
    end

    private

    def worker_executor(work)
      work.processor_type ? :"sidekiq_bolt_#{work.processor_type}" : :immediate
    end

  end
end
