module Sidekiq
  module Bolt
    class Message < Hash
      VALID_ATTRIBUTES = %w(class args queue resource jid pjid persist).freeze

      def marshal_dump
        values_at(*VALID_ATTRIBUTES)
      end

      def marshal_load(array)
        VALID_ATTRIBUTES.each.with_index { |key, index| self[key] = array[index] }
      end
    end
  end
end
