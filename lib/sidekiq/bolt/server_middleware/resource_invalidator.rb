module Sidekiq
  module Bolt
    module ServerMiddleware
      class ResourceInvalidator
        include Util

        def call(_, _, _)
          ThomasUtils::Future.immediate do
            yield
          end.fallback do |invalid_resource|
            if invalid_resource.is_a?(Exceptions::InvalidResource)
              invalid_resource.allocator.destroy(invalid_resource.resource)
            end
            ThomasUtils::Future.error(invalid_resource)
          end
        end

      end
    end
  end
end
