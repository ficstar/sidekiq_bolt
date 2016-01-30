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
          failed = false
          begin
            yield
          rescue
            failed = true
            raise
          ensure
            if job['jid'] && (failed || job['resource'] != Resource::ASYNC_LOCAL_RESOURCE)
              argv = [job['pjid'], job['jid'], job['resource']]
              argv << 'failed' if failed
              Bolt.redis do |redis|
                redis.eval(REMOVE_DEPENDENCY_SCRIPT, keys: NAMESPACE_KEY, argv: argv)
              end
            end
          end
        end

      end
    end
  end
end
