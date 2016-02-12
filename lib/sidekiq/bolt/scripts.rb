module Sidekiq
  module Bolt
    module Scripts
      class << self
        MUTEX = Mutex.new

        def run_script(name, script)
          Bolt.redis do |redis|
            yield redis, load_script(name, script)
          end
        end

        def included(base)
          base.extend(self)
        end

        private

        def load_script(name, script)
          return scripts[name] if scripts[name]
          synchronize do
            scripts[name] ||= Bolt.redis do |redis|
              redis.script(:load, script)
            end
          end
        end

        def scripts
          return @scripts if @scripts
          synchronize { @scripts ||= {} }
        end

        def synchronize(&block)
          MUTEX.synchronize(&block)
        end
      end

      def run_script(name, script, &block)
        Scripts.run_script(name, script, &block)
      end
    end
  end
end
