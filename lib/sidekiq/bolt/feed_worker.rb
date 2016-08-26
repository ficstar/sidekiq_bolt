module Sidekiq
  module Bolt
    module FeedWorker
      attr_accessor :channel

      def self.included(base)
        base.instance_eval { @sidekiq_options = {channel: 'global'} }
        base.extend(ClassMethods)
      end

      module ClassMethods
        def sidekiq_options(options)
          @sidekiq_options.merge!(options)
        end

        def perform_async(*args)
          perform_async_with_options({}, *args)
        end

        def perform_async_with_options(options, *args)
          publish_channel = options[:channel] || channel
          Bolt.redis { |redis| redis.publish(publish_channel, message(options, args)) }
        end

        private

        def message(options, args)
          item = {
              'class' => to_s,
              'args' => args
          }
          item['pid'] = options[:process_identity] if options[:process_identity]
          Sidekiq.dump_json(item)
        end

        def channel
          @sidekiq_options[:channel]
        end
      end

    end
  end
end
