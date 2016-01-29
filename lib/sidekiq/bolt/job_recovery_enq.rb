module Sidekiq
  module Bolt
    class JobRecoveryEnq

      def enqueue_jobs
        processes = Bolt.redis { |redis| redis.smembers('bolt:processes') }
        process_states = Bolt.redis do |redis|
          redis.multi do
            processes.each { |process| redis.exists("bolt:processes:#{process}") }
          end
        end
        processes.each.with_index do |process, index|
          ProcessSweeper.new(process).sweep unless process_states[index]
        end
      end

    end
  end
end
