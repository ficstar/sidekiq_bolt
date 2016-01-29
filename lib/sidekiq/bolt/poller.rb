module Sidekiq
  module Bolt
    class Poller
      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      POLL_SCRIPT_PATH = "#{SCRIPT_ROOT}/poll.lua"
      POLL_SCRIPT = File.read(POLL_SCRIPT_PATH)
      DEFROST_SCRIPT_PATH = "#{SCRIPT_ROOT}/defrost.lua"
      DEFROST_SCRIPT = File.read(DEFROST_SCRIPT_PATH)
      NAMESPACE_KEY = [''].freeze

      def initialize
        enq_klasses = [Scheduled::Enq] + Sidekiq.options.fetch(:additional_scheduled_enqs) { [] }
        @enqs = enq_klasses.map(&:new)
      end

      def enqueue_jobs
        @enqs.map(&:enqueue_jobs)
        {retry: 'retrying:', scheduled: ''}.each do |set, prefix|
          Bolt.redis do |redis|
            now = Time.now.to_f
            redis.eval(DEFROST_SCRIPT, keys: NAMESPACE_KEY, argv: [now])
            redis.eval(POLL_SCRIPT, keys: NAMESPACE_KEY, argv: ["bolt:#{set}", prefix, now])
          end
        end
      end
    end
  end
end
