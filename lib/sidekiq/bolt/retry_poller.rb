module Sidekiq
  module Bolt
    class RetryPoller < Scheduled::Poller
      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      POLL_SCRIPT_PATH = "#{SCRIPT_ROOT}/poll.lua"
      POLL_SCRIPT = File.read(POLL_SCRIPT_PATH)
      NAMESPACE_KEY = [''].freeze

      def enqueue
        Bolt.redis do |redis|
          redis.eval(POLL_SCRIPT, keys: NAMESPACE_KEY, argv: [Time.now.to_f])
        end
      end
    end
  end
end
