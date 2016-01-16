module Sidekiq
  module Bolt
    module ServerMiddleware
      class JobSuccession

        NAMESPACE_KEY = [''].freeze
        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        REMOVE_DEPENDENCY_SCRIPT_PATH = "#{SCRIPT_ROOT}/remove_dependency.lua"
        REMOVE_DEPENDENCY_SCRIPT = File.read(REMOVE_DEPENDENCY_SCRIPT_PATH)

        def call(_, job, _)
          yield
        ensure
          Bolt.redis do |redis|
            redis.eval(REMOVE_DEPENDENCY_SCRIPT, keys: NAMESPACE_KEY, argv: [job['pjid'], job['jid']])
          end
        end

      end
    end
  end
end
