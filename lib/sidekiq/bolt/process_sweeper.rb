module Sidekiq
  module Bolt
    class ProcessSweeper

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      NAMESPACE_KEY = [''].freeze
      SWEEP_SCRIPT_PATH = "#{SCRIPT_ROOT}/sweep.lua"
      SWEEP_SCRIPT = File.read(SWEEP_SCRIPT_PATH)

      def initialize(process)
        @process = process
      end

      def sweep
        Bolt.redis do |redis|
          redis.eval(SWEEP_SCRIPT, keys: NAMESPACE_KEY, argv: [process])
        end
      end

      private

      attr_reader :process

    end
  end
end
