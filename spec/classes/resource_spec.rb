require 'rspec'

module Sidekiq
  module Bolt
    describe Resource do

      let(:name) { 'resourceful' }

      subject { Resource.new(name) }

      describe '#type' do
        let(:type) { 'zippers' }

        before { subject.type = type }

        it 'should be the specified type' do
          expect(subject.type).to eq('zippers')
        end

        it 'should store the value in redis' do
          expect(global_redis.get('resource:type:resourceful')).to eq('zippers')
        end
      end

      describe '#limit' do
        let(:limit) { 5 }

        before { subject.limit = limit }

        it 'should be the specified limit' do
          expect(subject.limit).to eq(5)
        end

        it 'should store the value in redis' do
          expect(global_redis.get('resource:limit:resourceful')).to eq('5')
        end

        it 'should be an integer property' do
          expect(Resource.new(name).limit).to eq(5)
        end
      end

      describe '#allocated' do
        let(:busy) { 5 }

        before { global_redis.set("resource:allocated:#{name}", busy) }

        its(:allocated) { is_expected.to eq(5) }

        context 'with a different resource' do
          let(:busy) { 15 }
          let(:name) { 'really_busy' }

          its(:allocated) { is_expected.to eq(15) }
        end
      end

      describe '#allocate' do
        let(:amount) { 5 }
        let(:limit) { nil }

        before { subject.limit = limit }

        it 'should allocate the specified amount from the resource' do
          subject.allocate(amount)
          expect(subject.allocated).to eq(5)
        end

        context 'with a different workload' do
          let(:amount) { 17 }

          it 'should allocate the specified amount from the resource' do
            subject.allocate(amount)
            expect(subject.allocated).to eq(17)
          end
        end

        context 'when allocated multiple times' do
          it 'should allocate the specified amount from the resource' do
            2.times { subject.allocate(amount) }
            expect(subject.allocated).to eq(10)
          end
        end

        context 'with a limit specified' do
          let(:limit) { 5 }

          context 'when allocating more than the limit' do
            let(:amount) { 7 }

            it 'should allocate no more than the available amount of resources' do
              subject.allocate(amount)
              expect(subject.allocated).to eq(5)
            end

            context 'when called multiple times' do
              let(:amount) { 3 }

              it 'should allocate no more than the available amount of resources' do
                2.times { subject.allocate(amount) }
                expect(subject.allocated).to eq(5)
              end
            end

            context 'when the limit changes' do
              let(:amount) { 5 }

              it 'should leave the current allocation alone' do
                subject.allocate(amount)
                subject.limit = 1
                subject.allocate(amount)
                expect(subject.allocated).to eq(5)
              end
            end
          end
        end
      end

    end
  end
end
