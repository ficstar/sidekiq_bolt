module Sidekiq
  module Bolt
    class JobRecoveryEnq

      def enqueue_jobs
        processes = Bolt.redis { |redis| redis.smembers('bolt:processes') }
        Sidekiq.logger.debug "JobRecoveryEnq pulled process list: #{processes.inspect}"
        process_states = Bolt.redis do |redis|
          redis.multi do
            processes.each { |process| redis.exists("bolt:processes:#{process}") }
          end
        end
        processes.each.with_index do |process, index|
          unless process_states[index]
            Sidekiq.logger.debug "process '#{process}' has died, recovering its work"
            ProcessSweeper.new(process).sweep
          end
        end
      end

    end
  end
end
