require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch do

      let(:resource_type) { nil }
      let(:concurrency_pool) { {} }
      let(:concurrency) { 100 }
      let(:sidekiq_options) { {concurrency: concurrency, concurrency_pool: concurrency_pool} }
      let(:options) { sidekiq_options }

      subject { Fetch.new(options) }

      describe '.bulk_requeue' do
        subject { Fetch }
        it { is_expected.to respond_to(:bulk_requeue) }
      end

      describe '.local_queue' do
        it { expect(Fetch.local_queue).to be_a_kind_of(::Queue) }
      end

      describe 'initialization' do
        it 'should set up the global processor allocator' do
          subject
          expect(Fetch.processor_allocator).to be_a_kind_of(ProcessorAllocator)
        end
      end

      describe '#retrieve_work' do
        before { allow_any_instance_of(Fetch).to receive(:sleep).and_return(1) }

        its(:retrieve_work) { is_expected.to be_nil }

        it 'should sleep 1 second to wait for more work' do
          expect(subject).to receive(:sleep).with(1)
          subject.retrieve_work
        end

        context 'when there is work to be done' do
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:resource) { Resource.new(resource_name) }
          let(:work) { SecureRandom.uuid }
          let(:expected_work) { Fetch::UnitOfWork.new(queue_name, '-1', resource_name, work) }

          context 'when work has been scheduled locally' do
            before { Fetch.local_queue << expected_work }

            its(:retrieve_work) { is_expected.to eq(expected_work) }

            it 'should not sleep' do
              expect(subject).not_to receive(:sleep).with(1)
              subject.retrieve_work
            end
          end

          context 'when the work comes from a resource' do
            before { resource.add_work(queue_name, work) }

            its(:retrieve_work) { is_expected.to eq(expected_work) }

            context 'when a queue group filter has been provided' do
              let(:queue_group) { Faker::Lorem.sentence }
              let(:configured_queue_group) { Faker::Lorem.sentence }

              before do
                Queue.new(queue_name).group = queue_group
                sidekiq_options[:queue_group] = configured_queue_group
              end

              its(:retrieve_work) { is_expected.to be_nil }

              context 'when the groups match' do
                let(:queue_group) { configured_queue_group }
                its(:retrieve_work) { is_expected.to eq(expected_work) }
              end
            end

            it 'should not sleep' do
              expect(subject).not_to receive(:sleep).with(1)
              subject.retrieve_work
            end

            it 'should only allocate 1 worker' do
              subject.retrieve_work
              expect(Fetch.processor_allocator.allocation(resource_type)).to eq(1)
            end

            context 'when that work has already been consumed' do
              before { resource.allocate(1) }

              it 'should return the worker allocation' do
                subject.retrieve_work
                expect(Fetch.processor_allocator.allocation(resource_type)).to be_zero
              end
            end

            context 'with there are two resource and one has all its work consumed' do
              let(:resource_name_two) { Faker::Lorem.sentence }
              let(:resource_two) { Resource.new(resource_name_two) }
              let(:work_two) { SecureRandom.uuid }

              before do
                resource_two.add_work(queue_name, work_two)
                resource_two.allocate(1)
              end

              its(:retrieve_work) { is_expected.to eq(expected_work) }
            end

            context 'with more work available from the resource' do
              let(:work_two) { SecureRandom.uuid }
              let(:expected_work_two) { Fetch::UnitOfWork.new(queue_name, '-1', resource_name, work_two) }

              before { resource.add_work(queue_name, work_two) }

              its(:retrieve_work) { is_expected.to eq(expected_work_two) }

              it 'should store the work locally for later' do
                subject.retrieve_work
                expect(Fetch.local_queue.pop).to eq(expected_work)
              end

              context 'when we do not have enough workers' do
                let(:concurrency) { 1 }

                it 'should not allocate any extra' do
                  subject.retrieve_work
                  expect(Fetch.local_queue.size).to be_zero
                end
              end

              context 'when the resource has lots of work' do
                before do
                  25.times { resource.add_work(queue_name, SecureRandom.uuid) }
                end

                it 'should locally allocate as much as possible' do
                  subject.retrieve_work
                  expect(Fetch.local_queue.size).to eq(26)
                end
              end
            end

            context 'when we do not have enough workers' do
              let(:concurrency) { 0 }

              its(:retrieve_work) { is_expected.to be_nil }

              it 'should sleep' do
                expect(subject).to receive(:sleep)
                subject.retrieve_work
              end
            end
          end
        end

        context 'when configured with a resource filter' do
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_type) { Faker::Lorem.word }
          let(:resource) { Resource.new(Faker::Lorem.word).tap { |resource| resource.type = resource_type } }
          let(:resource_type_two) { Faker::Lorem.sentence }
          let(:resource_two) { Resource.new(Faker::Lorem.word).tap { |resource| resource.type = resource_type_two } }
          let(:concurrency_pool) { {resource_type => 100, resource_type_two => 100, nil => 100} }
          let(:sidekiq_options) { {concurrency: concurrency, resource_types: [resource_type], concurrency_pool: concurrency_pool} }

          let(:work) { SecureRandom.uuid }
          let(:work_two) { SecureRandom.uuid }
          let(:expected_processor_type) { resource_type }
          let(:expected_work) { Fetch::UnitOfWork.new(queue_name, '-1', resource.name, work, expected_processor_type) }

          before do
            resource.add_work(queue_name, work)
            resource_two.add_work(queue_name, work_two)
          end

          it 'should only pull work from the specified resource type' do
            expect(subject.retrieve_work).to eq(expected_work)
          end

          context 'when that work has already been consumed' do
            before { resource.allocate(1) }

            it 'should return the worker allocation' do
              subject.retrieve_work
              expect(Fetch.processor_allocator.allocation(resource_type)).to be_zero
            end
          end

          context 'when we do not have enough workers' do
            let(:concurrency_pool) { {resource_type => 0, resource_type_two => 0, nil => 0} }

            its(:retrieve_work) { is_expected.to be_nil }

            it 'should sleep' do
              expect(subject).to receive(:sleep)
              subject.retrieve_work
            end
          end
        end
      end

    end
  end
end
