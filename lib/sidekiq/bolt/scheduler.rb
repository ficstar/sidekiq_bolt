module Sidekiq
  module Bolt
    class Scheduler

      NAMESPACE_KEY = [''].freeze
      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      SCHEDULE_SCRIPT_PATH = "#{SCRIPT_ROOT}/schedule.lua"
      SCHEDULE_SCRIPT = File.read(SCHEDULE_SCRIPT_PATH)

      def initialize(prev_job)
        @prev_job_id = prev_job['jid']
        @items = []
      end

      def perform_after(worker_class, *args)
        perform_after_with_options({}, worker_class, *args)
      end

      def perform_after_with_options(options, worker_class, *args)
        new_job = {
            'class' => worker_class.to_s,
            'jid' => SecureRandom.base64(16),
            'args' => args,
            'retry' => true
        }.merge(worker_class.get_sidekiq_options)

        new_job['queue'] = options[:queue] if options[:queue]
        new_job['resource'] = options[:resource] if options[:resource]
        new_job['jid'] = options[:job_id] if options[:job_id]
        new_job['pjid'] = options[:parent_job_id] if options[:parent_job_id]

        new_job = Sidekiq.client_middleware.invoke(worker_class.to_s, new_job, new_job['queue']) { new_job }

        if new_job
          serialized_job = Sidekiq.dump_json(new_job)
          items.concat [new_job['queue'], new_job['resource'], serialized_job]
        end
      end

      def schedule!
        Bolt.redis do |redis|
          redis.eval(SCHEDULE_SCRIPT, keys: NAMESPACE_KEY, argv: [prev_job_id, *items])
        end
      end

      private

      attr_reader :prev_job_id, :items

      public

      alias :job_id :prev_job_id

    end
  end
end
