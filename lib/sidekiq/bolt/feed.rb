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
                clean_channel = channel.gsub(/^#{Regexp.escape(redis.namespace)}:/, '')
                begin
                  item = Sidekiq.load_json(message)
                  if !item['pid'] || item['pid'] == identity
                    worker = item['class'].constantize.new
                    worker.channel = clean_channel
                    worker.perform(*item['args'])
                  end
                rescue Exception => error
                  Sidekiq.logger.error "Error processing feed from channel '#{clean_channel}': #{error}\n#{error.backtrace * "\n"}"
                end
              end
            end
          end
        end
      end

    end
  end
end
