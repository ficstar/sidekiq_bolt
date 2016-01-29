module Sidekiq
  module Bolt
    class JobRecoveryEnq

      def enqueue_jobs
        processes = Bolt.redis { |redis| redis.smembers('bolt:processes') }
        processes.each do |process|
          alive = Bolt.redis { |redis| redis.get("bolt:processes:#{process}") }
          ProcessSweeper.new(process).sweep unless alive
        end
      end

    end
  end
end
