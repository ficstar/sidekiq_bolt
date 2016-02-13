module Sidekiq
  module Bolt
    module Worker
      attr_accessor :queue, :resource, :original_message, :parent_job_id, :child_scheduler

      ROOT = File.dirname(__FILE__)
      SCRIPT_ROOT = ROOT + '/' + File.basename(__FILE__, '.rb')
      ADD_SCHEDULED_SCRIPT_PATH = "#{SCRIPT_ROOT}/add_scheduled.lua"
      ADD_SCHEDULED_SCRIPT = File.read(ADD_SCHEDULED_SCRIPT_PATH)
      NAMESPACE_KEY = [''].freeze

      def self.included(base)
        base.send(:include, Sidekiq::Worker)
        base.extend ClassMethods
        base.class_attribute :sidekiq_should_retry_block
        base.class_attribute :sidekiq_freeze_resource_after_retry_for_block
      end

      module ClassMethods
        include Scripts

        def perform_async(*args, &block)
          #noinspection RubyStringKeysInHashInspection
          client_push({'class' => self, 'args' => args}, &block)
        end

        def perform_in(interval, *args)
          schedule_at = Time.now.to_f + interval
          job = get_sidekiq_options.merge('class' => self.to_s, 'args' => args)
          serialized_job = Sidekiq.dump_json(job)
          run_script(:worker_schedule, ADD_SCHEDULED_SCRIPT, NAMESPACE_KEY, [job['queue'], job['resource'], serialized_job, schedule_at])
        end

        def perform_async_with_options(options, *args, &block)
          #noinspection RubyStringKeysInHashInspection
          item = {
              'class' => self,
              'args' => args,
              'queue' => options[:queue],
              'resource' => options[:resource],
              'jid' => options[:job_id],
              'pjid' => options[:parent_job_id],
          }
          client_push(item, &block)
        end

        def sidekiq_should_retry?(&block)
          self.sidekiq_should_retry_block = block
        end

        #noinspection RubyInstanceMethodNamingConvention
        def sidekiq_freeze_resource_after_retry_for(&block)
          self.sidekiq_freeze_resource_after_retry_for_block = block
        end

        def get_sidekiq_options
          self.sidekiq_options_hash ||= Sidekiq.default_worker_options.merge('resource' => 'default')
        end

        private

        def client_push(item, &block)
          item['jid'] ||= SecureRandom.base64(16)
          item['pjid'] ||= get_sidekiq_options['queue']

          if block
            sheduler = Scheduler.new(item)
            block.call(sheduler)
            sheduler.schedule!
          end

          Sidekiq::Bolt::Client.new.push(item)
        end
      end

      def acknowledge_work(error = nil)
        if resource.name == Resource::ASYNC_LOCAL_RESOURCE
          job = error ? Sidekiq.load_json(original_message) : {'jid' => jid, 'pjid' => parent_job_id}
          ServerMiddleware::JobSuccession.new.call(self, job, nil) do
            ServerMiddleware::RetryJobs.new.call(self, job, nil) { raise error } if error
          end
        end
      rescue => error
        Sidekiq.logger.error("Local async job failed: #{error}\n#{error.backtrace * "\n"}")
      ensure
        fetched_work.force_acknowledge
      end

      private

      def fetched_work
        @fetched_work ||= Fetch::UnitOfWork.new(queue.name, resource.name, original_message)
      end

    end
  end
end
