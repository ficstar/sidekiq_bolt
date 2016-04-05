module Sidekiq
  class Processor

    def process(work)
      @boss.async.real_thread(proxy_id, Thread.current)
      msg = Sidekiq.load_json(work.message)
      worker_klass = msg['class'].constantize
      worker = worker_klass.new
      ThomasUtils::Future.immediate do
        Sidekiq.server_middleware.invoke(worker, msg, work.queue_name) do
          execute_job(worker, cloned(msg['args']))
          work.acknowledge
        end
      end
      @boss.async.processor_done(current_actor)
    end

  end
end
