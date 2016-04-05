module Sidekiq
  module Bolt
    module ServerMiddleware
      class TypeSafety

        def call(_, job, _)
          ThomasUtils::Future.immediate do
            job['args'].each.with_index do |arg, index|
              job['args'][index] = arg.to_time if arg.is_a?(EncodedTime)
            end
          end.then { yield }
        end

      end
    end
  end
end
