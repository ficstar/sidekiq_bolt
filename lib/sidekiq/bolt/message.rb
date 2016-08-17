module Sidekiq
  module Bolt
    class Message < Hash
      VALID_ATTRIBUTES = %w{
                              class
                              args
                              queue
                              resource
                              jid
                              pjid
                              persist
                              enqueued_at
                              job
                              error
                              retry
                              retry_count
                        }.freeze
      VALID_ATTRIBUTE_SET = Set.new(VALID_ATTRIBUTES)

      class UnsupportedKeyError < StandardError
        def initialize
          super('Unsupported message key!')
        end
      end

      def []=(key, value)
        raise UnsupportedKeyError unless VALID_ATTRIBUTE_SET.include?(key)
        super(key, value)
      end

      def [](key)
        raise UnsupportedKeyError unless VALID_ATTRIBUTE_SET.include?(key)
        super(key)
      end

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
