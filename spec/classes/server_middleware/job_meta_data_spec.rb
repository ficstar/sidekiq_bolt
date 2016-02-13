require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe JobMetaData do

        describe '#call' do
          let(:worker) { Struct.new(:queue, :resource, :jid, :parent_job_id, :original_message, :child_scheduler).new }
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:job_id) { SecureRandom.uuid }
          let(:parent_job_id) { SecureRandom.uuid }
          #noinspection RubyStringKeysInHashInspection
          let(:job) { {'queue' => queue_name, 'resource' => resource_name, 'jid' => job_id, 'pjid' => parent_job_id} }
          let(:child_scheduler) { double(:scheduler, :schedule! => nil) }
          let(:original_message) { Sidekiq.load_json(subject.original_message) }

          before { allow(ChildScheduler).to receive(:new).with(job).and_return(child_scheduler) }

          it 'should yield' do
            expect { |block| subject.call(worker, job, nil, &block) }.to yield_control
          end

          it 'should schedule any child jobs after the work is done' do
            subject.call(worker, job, nil) do
              expect(child_scheduler).to receive(:schedule!)
            end
          end

          describe 'setting meta data' do
            let(:middleware) { JobMetaData.new }
            subject { worker }
            before { middleware.call(worker, job, nil) {} }

            its(:queue) { is_expected.to eq(Queue.new(queue_name)) }
            its(:resource) { is_expected.to eq(Resource.new(resource_name)) }
            its(:parent_job_id) { is_expected.to eq(parent_job_id) }
            its(:jid) { is_expected.to eq(job_id) }
            its(:child_scheduler) { is_expected.to eq(child_scheduler) }
            it { expect(original_message).to eq(job) }
          end

        end

      end
    end
  end
end
