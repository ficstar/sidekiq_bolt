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

      def self.invoke_handler(worker, job, error)
        error_klass = error.class
        error_handler = Sidekiq.options[:error_handlers] && Sidekiq.options[:error_handlers].find do |handler_klass, _|
          error_klass <= handler_klass
        end
        if error_handler
          _, handler = error_handler
          handler.call(worker, job, error)
          true
        else
          false
        end
      end
    end
  end
end
