module Sidekiq
  module Bolt
    module Serializer

      MAGIC = 'MRSH'.encode('ASCII-8BIT')

      def dump_json(value)
        MAGIC + Marshal.dump(value)
      end

      def load_json(dump)
        validate_dump_header!(dump)
        Marshal.load(dump[4..-1])
      end

      private

      def validate_dump_header!(dump)
        raise 'Invalid Marshal dump provided' unless dump.is_a?(String) && dump[0...4] == MAGIC
      end

    end
  end
end
