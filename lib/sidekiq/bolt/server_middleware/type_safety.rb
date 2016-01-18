module Sidekiq
  module Bolt
    module ServerMiddleware
      class TypeSafety

        def call(_, job, _)
          job['args'].each.with_index do |arg, index|
            job['args'][index] = arg.to_time if arg.is_a?(EncodedTime)
          end
          yield
        end

      end
    end
  end
end
