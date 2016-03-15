module Sidekiq
  module Bolt
    module ServerMiddleware
      class ResourceInvalidator
        include Util

        def call(_, _, _)
          yield
        rescue Exceptions::InvalidResource => invalid_resource
          invalid_resource.allocator.destroy(invalid_resource.resource)
          raise invalid_resource
        end

      end
    end
  end
end
