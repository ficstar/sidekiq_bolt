module Sidekiq
  module Bolt
    class ProcessSweeper
      include Scripts

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      NAMESPACE_KEY = [''].freeze
      SWEEP_SCRIPT_PATH = "#{SCRIPT_ROOT}/sweep.lua"
      SWEEP_SCRIPT = File.read(SWEEP_SCRIPT_PATH)

      def initialize(process)
        @process = process
      end

      def sweep
        run_script(:sweeper_sweep, SWEEP_SCRIPT, NAMESPACE_KEY, [process])
      end

      private

      attr_reader :process

    end
  end
end
