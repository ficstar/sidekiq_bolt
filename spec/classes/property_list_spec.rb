require 'rspec'

describe Sidekiq::Bolt::PropertyList do

  let(:test_list) do
    Struct.new(:name) do
      include Sidekiq::Bolt::PropertyList
    end
  end

  describe '#define_property' do

  end

end
