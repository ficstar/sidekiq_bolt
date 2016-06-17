module Sidekiq
  module Bolt
    module ServerMiddleware
      class Statistics
        include Scripts
        include ThomasUtils::PerformanceMonitorMixin

        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        COUNT_STATS_SCRIPT_PATH = "#{SCRIPT_ROOT}/stats.lua"
        COUNT_STATS_SCRIPT = File.read(COUNT_STATS_SCRIPT_PATH)
        NAMESPACE_KEY = [''].freeze

        def call(_, job, _)
          future = ThomasUtils::Future.none.then { yield }.on_success_ensure do
            run_script(:stats_count, COUNT_STATS_SCRIPT, NAMESPACE_KEY, [job['resource'], job['queue']])
          end.on_failure_ensure do
            run_script(:stats_count, COUNT_STATS_SCRIPT, NAMESPACE_KEY, [job['resource'], job['queue'], true])
          end
          monitor_performance(__method__, {name: :bolt_statistics, worker_class: job['class'], queue: job['queue'], resource: job['resource']}, future)
        end

      end
    end
  end
end
