module Sidekiq
  module Bolt
    module FeedWorker

      def self.included(base)
        base.instance_eval { @sidekiq_options = {channel: 'global'} }
        base.extend(ClassMethods)
      end

      module ClassMethods
        def sidekiq_options(options)
          @sidekiq_options.merge!(options)
        end

        def perform_async(*args)
          Bolt.redis { |redis| redis.publish(channel, message(args)) }
        end

        private

        def message(args)
          Sidekiq.dump_json('class' => to_s, 'args' => args)
        end

        def channel
          @sidekiq_options[:channel]
        end
      end

    end
  end
end
