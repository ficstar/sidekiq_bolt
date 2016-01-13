require 'rspec'

describe Sidekiq::Bolt::PropertyList do

  let(:name) { 'list' }
  let(:property_klass) do
    Struct.new(:name) do
      extend Sidekiq::Bolt::PropertyList
    end
  end
  subject { property_klass.new(name) }

  describe '.define_property' do

    describe 'setting the property' do
      it 'should define a method to access attributes from redis' do
        property_klass.define_property(:queue_type, :type)
        subject.type = 'Q'
        expect(global_redis.get('queue_type:list')).to eq('Q')
      end

      context 'with a different property' do
        it 'should define a method to access attributes from redis' do
          property_klass.define_property(:meta_data, :project_name)
          subject.project_name = 'Big Worker Man'
          expect(global_redis.get('meta_data:list')).to eq('Big Worker Man')
        end
      end

      context 'with a different property list name' do
        let(:name) { 'list_two' }

        it 'should define a method to access attributes from redis' do
          property_klass.define_property(:meta_data, :project_name)
          subject.project_name = 'Big Worker Man'
          expect(global_redis.get('meta_data:list_two')).to eq('Big Worker Man')
        end
      end
    end

    describe 'retrieving the property' do
      it 'should define a method to retrieve a property from redis' do
        property_klass.define_property(:queue_type, :type)
        global_redis.set('queue_type:list', 'L')
        expect(subject.type).to eq('L')
      end

      it 'should cache the value' do
        property_klass.define_property(:queue_type, :type)
        global_redis.set('queue_type:list', 'L')
        subject.type
        expect_any_instance_of(Redis).not_to receive(:get)
        subject.type
      end

      context 'with a different property' do
        it 'should define a method to retrieve a property from redis' do
          property_klass.define_property(:meta_data, :project_name)
          global_redis.set('meta_data:list', 'The Do Everything Project')
          expect(subject.project_name).to eq('The Do Everything Project')
        end

        it 'should cache the right value' do
          property_klass.define_property(:queue_type, :type)
          property_klass.define_property(:meta_data, :project_name)
          global_redis.set('queue_type:list', 'L')
          global_redis.set('meta_data:list', 'The Do Everything Project')
          subject.type
          expect(subject.project_name).to eq('The Do Everything Project')
        end
      end
    end

    it 'should cache the property when setting it' do
      property_klass.define_property(:queue_type, :type)
      subject.type = 'Q'
      expect_any_instance_of(Redis).not_to receive(:get)
      subject.type
    end

    context 'with a different property' do
      it 'should cache the right value' do
        property_klass.define_property(:queue_type, :type)
        property_klass.define_property(:meta_data, :project_name)
        subject.type = 'Q'
        subject.project_name = 'Big Worker Man'
        expect(subject.type).to eq('Q')
      end
    end

    context 'with an integer property' do
      it 'should keep the value as an integer' do
        property_klass.define_property(:queue_busy, :busy, :int)
        subject.busy = 5
        expect(property_klass.new(name).busy).to eq(5)
      end
    end

    context 'with a counter property' do
      let(:property) { :busy }

      before { property_klass.define_property(:queue_busy, property, :counter) }

      it 'should should define a method to increment the counter' do
        subject.busy_incr
        expect(property_klass.new(name).busy).to eq(1)
      end

      it 'should should define a method to increment the counter by a specified number' do
        subject.busy_incrby(5)
        expect(property_klass.new(name).busy).to eq(5)
      end

      it 'should should define a method to decrement the counter' do
        subject.busy_decr
        expect(property_klass.new(name).busy).to eq(-1)
      end

      it 'should should define a method to decrement the counter by a specified number' do
        subject.busy_decrby(6)
        expect(property_klass.new(name).busy).to eq(-6)
      end

      context 'with a different property' do
        let(:property) { :errors }

        it 'should should define a method to increment the counter' do
          subject.errors_incr
          expect(property_klass.new(name).errors).to eq(1)
        end

        it 'should should define a method to increment the counter by a specified number' do
          subject.errors_incrby(15)
          expect(property_klass.new(name).errors).to eq(15)
        end

        it 'should should define a method to decrement the counter' do
          subject.errors_decr
          expect(property_klass.new(name).errors).to eq(-1)
        end

        it 'should should define a method to decrement the counter by a specified number' do
          subject.errors_decrby(9)
          expect(property_klass.new(name).errors).to eq(-9)
        end
      end
    end
  end

end
