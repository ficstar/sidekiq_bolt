module Sidekiq
  class Processor

    def process(work)
      @boss.async.real_thread(proxy_id, Thread.current)
      msg = Sidekiq.load_json(work.message)
      worker_klass = msg['class'].constantize
      worker = worker_klass.new
      ack = false
      future = ThomasUtils::Future.value(nil).then do
        Sidekiq.server_middleware.invoke(worker, msg, work.queue_name) do
          ack = true
          execute_job(worker, cloned(msg['args']))
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

  end
end
