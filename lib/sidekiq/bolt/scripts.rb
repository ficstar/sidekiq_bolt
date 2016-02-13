module Sidekiq
  module Bolt
    module Scripts
      class << self
        MUTEX = Mutex.new

        def included(base)
          base.extend(self)
        end

        def run_script(redis, name, script, keys, argv)
          redis.evalsha(load_script(name, script), keys: keys, argv: argv)
        rescue Redis::CommandError => error
          if error.message =~ /NOSCRIPT/
            dump_script(name)
            retry
          else
            raise
          end
        end

        def load_script(name, script)
          return scripts[name] if scripts[name]
          synchronize do
            scripts[name] ||= Bolt.redis do |redis|
              redis.script(:load, script)
            end
          end
        end

        private

        def dump_script(name)
          synchronize { scripts.delete(name) }
        end

        def scripts
          return @scripts if @scripts
          synchronize { @scripts ||= {} }
        end

        def synchronize(&block)
          MUTEX.synchronize(&block)
        end
      end

      def run_script(name, script, keys, argv)
        Bolt.redis { |redis| internal_run_script(redis, name, script, keys, argv) }
      end

      private

      def internal_run_script(redis, name, script, keys, argv)
        Scripts.run_script(redis, name, script, keys, argv)
      end
    end
  end
end
