require 'rspec'

describe Sidekiq::Bolt::PropertyList do

  let(:property_klass) do
    Struct.new(:name) do
      extend Sidekiq::Bolt::PropertyList
    end
  end
  subject { property_klass.new }

  describe '.define_property' do

    xit 'should define a method to access attributes from redis' do
      property_klass.define_property(:queue, :type)
      subject.type = 'Q'
      expect(global_redis.get('queue:type')).to eq('Q')
    end

  end

end
