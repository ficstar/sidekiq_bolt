module Sidekiq
  module Bolt
    module ClientMiddleware
      class TypeSafety

        def call(_, job, _, _ = nil)
          job['args'].each.with_index do |arg, index|
            job['args'][index] = EncodedTime.new(arg.to_f) if arg.is_a?(Time)
          end
          yield
        end

      end
    end
  end
end
