module Sidekiq
  module Bolt
    class Message < Hash
      VALID_ATTRIBUTES = %w(class args queue resource jid pjid persist enqueued_at job retry).freeze

      def marshal_dump
        values_at(*VALID_ATTRIBUTES)
      end

      def marshal_load(array)
        VALID_ATTRIBUTES.each.with_index do |key, index|
          value = array[index]
          self[key] = value unless value.nil?
        end
      end
    end
  end
end
