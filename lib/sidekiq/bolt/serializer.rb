module Sidekiq
  MARSHAL_MAGIC = 'MRSH'.encode('ASCII-8BIT')

  class << self
    def dump_json(value)
      MARSHAL_MAGIC + Marshal.dump(value)
    end

    def load_json(dump)
      validate_serialized_dump_header!(dump)
      Marshal.load(dump[4..-1])
    end

    private

    def validate_serialized_dump_header!(dump)
      raise 'Invalid Marshal dump provided' unless dump.is_a?(String) && dump[0...4] == MARSHAL_MAGIC
    end
  end

end
