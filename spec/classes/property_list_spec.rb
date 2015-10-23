require 'rspec'

describe Sidekiq::Bolt::PropertyList do

  let(:property_klass) do
    Struct.new(:name) do
      extend Sidekiq::Bolt::PropertyList
    end
  end
  subject { property_klass.new }

  describe '.define_property' do

    describe 'setting the property' do
      it 'should define a method to access attributes from redis' do
        property_klass.define_property(:queue, :type)
        subject.type = 'Q'
        expect(global_redis.get('queue:type')).to eq('Q')
      end

      context 'with a different property' do
        it 'should define a method to access attributes from redis' do
          property_klass.define_property(:meta_data, :project_name)
          subject.project_name = 'Big Worker Man'
          expect(global_redis.get('meta_data:project_name')).to eq('Big Worker Man')
        end
      end
    end

    describe 'retrieving the property' do
      it 'should define a method to retrieve a property from redis' do
        property_klass.define_property(:queue, :type)
        global_redis.set('queue:type', 'L')
        expect(subject.type).to eq('L')
      end

      it 'should cache the value' do
        property_klass.define_property(:queue, :type)
        global_redis.set('queue:type', 'L')
        subject.type
        expect_any_instance_of(Redis).not_to receive(:get)
        subject.type
      end

      context 'with a different property' do
        it 'should define a method to retrieve a property from redis' do
          property_klass.define_property(:meta_data, :project_name)
          global_redis.set('meta_data:project_name', 'The Do Everything Project')
          expect(subject.project_name).to eq('The Do Everything Project')
        end

        it 'should cache the right value' do
          property_klass.define_property(:queue, :type)
          property_klass.define_property(:meta_data, :project_name)
          global_redis.set('queue:type', 'L')
          global_redis.set('meta_data:project_name', 'The Do Everything Project')
          subject.type
          expect(subject.project_name).to eq('The Do Everything Project')
        end
      end
    end

    it 'should cache the property when setting it' do
      property_klass.define_property(:queue, :type)
      subject.type = 'Q'
      expect_any_instance_of(Redis).not_to receive(:get)
      subject.type
    end

    context 'with a different property' do
      it 'should cache the right value' do
        property_klass.define_property(:queue, :type)
        property_klass.define_property(:meta_data, :project_name)
        subject.type = 'Q'
        subject.project_name = 'Big Worker Man'
        expect(subject.type).to eq('Q')
      end
    end
  end

end
