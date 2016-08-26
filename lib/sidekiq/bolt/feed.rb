module Sidekiq
  module Bolt
    class Feed
      include Util

      def initialize(options)
        @channels = options[:subscribed_channels] || ['global']
        ThomasUtils::Future.new do
          Bolt.redis do |redis|
            redis.subscribe(*@channels) do |on|
              on.message do |channel, message|
                item = Sidekiq.load_json(message)
                if !item['pid'] || item['pid'] == identity
                  worker = item['class'].constantize.new
                  worker.channel = channel.gsub(/^#{Regexp.escape(redis.namespace)}:/, '')
                  worker.perform(*item['args'])
                end
              end
            end
          end
        end
      end

    end
  end
end
