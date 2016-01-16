require 'rspec'

module Sidekiq
  module Bolt
    describe Fetch do

      let(:options) { {} }

      subject { Fetch.new(options) }

      after { Fetch.instance_variable_set(:@processor_allocator, nil) }

      describe '.bulk_requeue' do
        subject { Fetch }
        it { is_expected.to respond_to(:bulk_requeue) }
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
          let(:work) { SecureRandom.uuid }
          let(:expected_work) { Fetch::UnitOfWork.new(queue_name, resource_name, work) }

          before { Resource.new(resource_name).add_work(queue_name, work) }

          its(:retrieve_work) { is_expected.to eq(expected_work) }

          it 'should not sleep' do
            expect(subject).not_to receive(:sleep).with(1)
            subject.retrieve_work
          end
        end

        context 'when provided with a filter' do
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_type) { Faker::Lorem.word }
          let(:resource) { Resource.new(Faker::Lorem.word).tap { |resource| resource.type = resource_type } }
          let(:resource_type_two) { Faker::Lorem.word }
          let(:resource_two) { Resource.new(Faker::Lorem.word).tap { |resource| resource.type = resource_type_two } }
          let(:options) { {resource_types: [resource_type]} }

          let(:work) { SecureRandom.uuid }
          let(:work_two) { SecureRandom.uuid }
          let(:expected_work) { Fetch::UnitOfWork.new(queue_name, resource.name, work) }

          before do
            resource.add_work(queue_name, work)
            resource_two.add_work(queue_name, work_two)
          end

          it 'should only pull work from the specified resource type' do
            expect(subject.retrieve_work).to eq(expected_work)
          end
        end
      end

    end
  end
end
