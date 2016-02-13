module Sidekiq
  module Bolt
    module ClientMiddleware
      class JobSuccession
        include Scripts

        class DuplicateJobError < StandardError
          def initialize(job_id, parent_job_id)
            super("Attempted to enqueue duplicate job '#{job_id}' with parent '#{parent_job_id}'")
          end
        end

        NAMESPACE_KEY = [''].freeze
        ROOT = File.dirname(__FILE__)
        SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
        ADD_DEPENDENCY_SCRIPT_PATH = "#{SCRIPT_ROOT}/add_dependency.lua"
        ADD_DEPENDENCY_SCRIPT = File.read(ADD_DEPENDENCY_SCRIPT_PATH)

        def call(_, msg, _, _ = nil)
          unless msg['error']
            parent_set = run_script(:job_succession_add_dependency, ADD_DEPENDENCY_SCRIPT, NAMESPACE_KEY, [msg['pjid'], msg['jid']])
            raise DuplicateJobError.new(msg['jid'], msg['pjid']) unless parent_set
          end
          yield
        end

      end
    end
  end
end
