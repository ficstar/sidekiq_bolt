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
          let(:resource_name) { :some_resource }
          let(:options) { {concurrency_pool: {some_resource: concurrency}} }
          it_behaves_like 'allocating for a resource type', :some_resource
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
          let(:resource_name) { Faker::Lorem.word.to_sym }
          let(:options) { {concurrency_pool: {resource_name => concurrency}} }
          let(:resources_to_free) { rand(1...concurrency) }

          before do
            allocator.allocate(concurrency, resource_name)
            allocator.free(resources_to_free, resource_name)
          end

          subject { allocator.allocation(resource_name) }

          it { is_expected.to eq(concurrency - resources_to_free) }
        end
      end

    end
  end
end
