# encoding: utf-8
require 'sidekiq/manager'

module Sidekiq
  class Manager
    def bolt_heartbeat(identity, serialized_info)
      Bolt.redis do |conn|
        conn.pipelined do
          conn.sadd('bolt:processes', identity)
          conn.set("bolt:processes:#{identity}", serialized_info)
          conn.expire("bolt:processes:#{identity}", 60)
        end
      end
    end

    alias :'❤' :bolt_heartbeat
  end
end
