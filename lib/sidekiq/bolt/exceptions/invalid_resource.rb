module Sidekiq
  module Bolt
    module Exceptions
      class InvalidResource < StandardError
        attr_reader :type, :resource

        def initialize(type, resource)
          @type = type
          @resource = resource
          super(%Q{Resource "#{resource}" of type "#{type}" has been invalidated!})
        end
      end
    end
  end
end
