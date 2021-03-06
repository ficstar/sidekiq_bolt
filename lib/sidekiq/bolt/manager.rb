# encoding: utf-8
require 'sidekiq/manager'

module Sidekiq
  module Bolt
    module Heartbeat

      def bolt_heartbeat(identity, serialized_info)
        Bolt.redis do |conn|
          conn.pipelined do
            conn.sadd('bolt:processes', identity)
            conn.set("bolt:processes:#{identity}", serialized_info)
            conn.expire("bolt:processes:#{identity}", 60)
          end
        end
      end

    end
  end

  class Manager
    include Bolt::Heartbeat

    def heartbeat(identity, info, serialized_info)
      bolt_heartbeat(identity, serialized_info)
      after(5) { heartbeat(identity, info, serialized_info) }
    end
  end
end
