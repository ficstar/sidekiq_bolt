module Sidekiq
  module Bolt
    module ErrorHandler
      MUTEX = Mutex.new

      def self.included(base)
        base.extend(ClassMethods)
      end

      module ClassMethods
        def register(error_klass)
          MUTEX.synchronize do
            Sidekiq.options[:error_handlers] ||= {}
            Sidekiq.options[:error_handlers][error_klass] = new
          end
        end
      end
    end
  end
end
