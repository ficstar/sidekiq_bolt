module Sidekiq
  module Bolt
    module Exceptions
      class InvalidResource < StandardError
        attr_reader :resource

        def initialize(resource)
          @resource = resource
          super(%Q{Resource "#{@resource}" has been invalidated!})
        end
      end
    end
  end
end
