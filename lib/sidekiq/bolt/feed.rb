module Sidekiq
  module Bolt
    class Feed
      include Util

      def initialize(options)
        @channels = options[:subscribed_channels] || ['global']
        ThomasUtils::Future.new do
          Bolt.redis do |redis|
            @redis = redis
            redis.subscribe(*@channels) do |on|
              on.message(&method(:handle_message))
            end
          end
        end
      end

      private

      attr_reader :redis

      def handle_message(channel, message)
        clean_channel = clean_channel(channel)
        begin
          process_message(clean_channel, message)
        rescue Exception => error
          Sidekiq.logger.error "Error processing feed from channel '#{clean_channel}': #{error}\n#{error.backtrace * "\n"}"
        end
      end

      def process_message(clean_channel, message)
        item = Sidekiq.load_json(message)
        perform_work(clean_channel, item) if supports_work?(item)
      end

      def supports_work?(item)
        !item['pid'] || item['pid'] == identity
      end

      def perform_work(clean_channel, item)
        worker = item['class'].constantize.new
        worker.channel = clean_channel
        worker.perform(*item['args'])
      end

      def clean_channel(channel)
        redis.is_a?(Redis::Namespace) ? remove_namespace(channel) : channel
      end

      def remove_namespace(channel)
        channel.gsub(/^#{Regexp.escape(redis.namespace)}:/, '')
      end

    end
  end
end
