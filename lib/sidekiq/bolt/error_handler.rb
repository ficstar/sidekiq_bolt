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

      class << self
        def invoke_handler(worker, job, error)
          error_handler = error_handler(error.class)
          if error_handler
            call_handler(error, error_handler, job, worker)
            true
          else
            false
          end
        end

        private

        def call_handler(error, error_handler, job, worker)
          _, handler = error_handler
          handler.call(worker, job, error)
        end

        def error_handler(error_klass)
          error_handlers && error_handlers.find do |handler_klass, _|
            error_klass <= handler_klass
          end
        end

        def error_handlers
          Sidekiq.options[:error_handlers]
        end
      end
    end
  end
end
