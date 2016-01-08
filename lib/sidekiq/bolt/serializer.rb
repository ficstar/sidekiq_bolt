module Sidekiq
  module Bolt
    module Serializer

      MAGIC = 'MRSH'.encode('ASCII-8BIT')

      def dump_json(value)
        MAGIC + Marshal.dump(value)
      end

      def load_json(value)
        raise 'Invalid Marshal dump provided' if value[0...4] != MAGIC
        Marshal.load(value[4..-1])
      end

    end
  end
end
