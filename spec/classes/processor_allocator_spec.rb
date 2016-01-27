require 'rspec'

module Sidekiq
  module Bolt
    describe ProcessorAllocator do

      let(:options) { {} }
      let(:allocator) { ProcessorAllocator.new(options) }

      describe '#allocate' do
        shared_examples_for 'allocating for a resource type' do |type|
          let(:allocation) { 1 }
          let(:concurrency) { 12 }

          subject do
            type ? allocator.allocate(allocation, type) : allocator.allocate(allocation)
          end

          it { is_expected.to eq(1) }

          context 'when called multiple times' do
            before { allocator.allocate(allocation, type) }
            it { is_expected.to eq(1) }
          end

          context 'when called with a different allocation' do
            let(:allocation) { 5 }
            it { is_expected.to eq(5) }
          end

          context 'when there are not enough workers' do
            let(:concurrency) { 0 }
            it { is_expected.to eq(0) }

            context 'when we can allocate some workers' do
              let(:concurrency) { 1 }
              let(:allocation) { 2 }

              it { is_expected.to eq(1) }
            end
          end

          context 'when called from multiple places' do
            let(:concurrency) { 5 }
            let(:allocation) { 3 }
            let!(:first_count) { allocator.allocate(5, type) }
            let!(:second_count) { allocator.allocate(5, type) }

            it 'should divide the workers between the two results' do
              expect(first_count + second_count).to eq(5)
            end
          end
        end

        context 'with a global resource pool' do
          let(:options) { {concurrency: concurrency} }
          it_behaves_like 'allocating for a resource type'
        end

        context 'with a specific resource' do
          let(:resource_type) { :some_resource }
          let(:options) { {concurrency: concurrency, concurrency_pool: {some_resource: concurrency}} }
          it_behaves_like 'allocating for a resource type', :some_resource

          describe 'allocating from the global pool' do
            let(:concurrency) { 1 }
            let(:allocation) { 1 }

            context 'when each resource consumes the entire pool' do
              it 'should have no resources left' do
                expect(allocator.allocate(allocation)).to be_zero
              end
            end

            context 'when there are resources left for the default type' do
              let(:concurrency) { 2 }
              let(:options) { {concurrency: concurrency, concurrency_pool: {some_resource: 1}} }

              it 'should allow allocation' do
                expect(allocator.allocate(allocation)).to eq(1)
              end

              context 'with multiple resources' do
                let(:options) { {concurrency: concurrency, concurrency_pool: {some_resource: 1, some_other_resource: 1}} }

                it 'should have no resources left' do
                  expect(allocator.allocate(allocation)).to be_zero
                end
              end
            end

          end
        end

        describe 'allocating from $async_local resource' do
          let(:allocation) { rand(1..999999) }
          subject { allocator.allocate(allocation, Resource::ASYNC_LOCAL_RESOURCE) }
          it { is_expected.to eq(allocation) }
        end

      end

      describe '#free' do
        let(:concurrency) { 12 }

        context 'with a global resource pool' do
          let(:options) { {concurrency: concurrency} }
          let(:resources_to_free) { rand(1...concurrency) }

          before do
            allocator.allocate(concurrency)
            allocator.free(resources_to_free)
          end

          subject { allocator.allocation }

          it { is_expected.to eq(concurrency - resources_to_free) }
        end

        context 'with a specific resource' do
          let(:resource_type) { Faker::Lorem.word.to_sym }
          let(:options) { {concurrency: concurrency, concurrency_pool: {resource_type => concurrency}} }
          let(:resources_to_free) { rand(1...concurrency) }

          subject { allocator.allocation(resource_type) }

          before do
            allocator.allocate(concurrency, resource_type)
            allocator.free(resources_to_free, resource_type)
          end

          it { is_expected.to eq(concurrency - resources_to_free) }
        end

        context 'with the $async_local resource' do
          let(:resource_type) { Resource::ASYNC_LOCAL_RESOURCE }
          let(:allocation) { rand(1..999999) }

          subject { allocator.allocation(resource_type) }

          before do
            allocator.allocate(allocation, resource_type)
            allocator.free(999999, resource_type)
          end

          it { is_expected.to eq(0) }
        end
      end

    end
  end
end
