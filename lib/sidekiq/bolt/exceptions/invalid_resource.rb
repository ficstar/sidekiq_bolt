module Sidekiq
  module Bolt
    module Exceptions
      class InvalidResource < StandardError
        attr_reader :allocator, :resource

        def initialize(allocator, resource)
          @allocator = allocator
          @resource = resource
          super(%Q{Resource "#{@resource}" of type "#{@allocator.name}" has been invalidated!})
        end
      end
    end
  end
end
