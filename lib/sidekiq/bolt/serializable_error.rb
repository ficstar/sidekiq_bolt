module Sidekiq
  module Bolt
    class SerializableError < StandardError
      attr_reader :error_class

      def initialize(error)
        @error_class = error.class
        super(error.message)
        set_backtrace(error.backtrace)
      end
    end
  end
end
