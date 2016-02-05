module Sidekiq
  module Bolt
    module ServerMiddleware
      class Statistics

        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        COUNT_STATS_SCRIPT_PATH = "#{SCRIPT_ROOT}/stats.lua"
        COUNT_STATS_SCRIPT = File.read(COUNT_STATS_SCRIPT_PATH)
        NAMESPACE_KEY = [''].freeze

        def call(_, job, _)
          yield
          Bolt.redis do |redis|
            redis.eval(COUNT_STATS_SCRIPT, keys: NAMESPACE_KEY, argv: [job['resource'], job['queue']])
          end
        rescue
          Bolt.redis do |redis|
            redis.eval(COUNT_STATS_SCRIPT, keys: NAMESPACE_KEY, argv: [job['resource'], job['queue'], true])
          end
          raise
        end

      end
    end
  end
end
