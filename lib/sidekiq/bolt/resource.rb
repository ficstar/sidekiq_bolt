module Sidekiq
  module Bolt
    class Resource < Struct.new(:name)
      extend PropertyList

      define_property :resource_type, :type
      define_property :resource_limit, :limit, :int

    end
  end
end
