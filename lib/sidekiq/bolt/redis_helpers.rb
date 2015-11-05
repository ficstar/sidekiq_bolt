module Sidekiq
  module Bolt

    def self.redis(&block)
      Sidekiq.redis(&block)
    end

  end
end
