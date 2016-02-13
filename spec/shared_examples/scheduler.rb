module Sidekiq
  module Bolt
    shared_examples_for 'a job scheduler' do

      let(:worker_class_name) { worker_class.to_s }
      let(:worker_class_base) do
        Class.new do
          include Worker

          def self.to_s
            @name ||= Faker::Lorem.word
          end
        end
      end
      let(:worker_class) { worker_class_base }
      let(:job_id) { SecureRandom.uuid }
      let(:parent) { {'jid' => job_id} }
      let(:scheduler) { klass.new(parent) }
      let(:args) { Faker::Lorem.paragraphs }
      let(:new_jid) { SecureRandom.uuid }
      let(:result_queue) { result_item['queue'] if result_item }
      let(:result_resource) { result_item['resource'] if result_item }
      let(:middleware) { double(:middleware) }
      let(:middleware_block) { ->(_, _, _, &block) { block.call } }
      let(:block) { nil }

      subject { scheduler }

      before do
        allow(SecureRandom).to receive(:base64).with(16).and_return(new_jid)
        allow(Sidekiq).to receive(:client_middleware).and_return(middleware)
        allow(middleware).to receive(:invoke) do |worker, job, queue, &block|
          middleware_block.call(worker, job, queue, &block)
        end
      end

      its(:job_id) { is_expected.to eq(job_id) }

      shared_examples_for 'a method scheduling a worker' do
        #noinspection RubyStringKeysInHashInspection
        let(:expected_work) do
          {
              'class' => worker_class_name,
              'jid' => new_jid,
              'queue' => 'default',
              'resource' => 'default',
              'args' => args,
              'retry' => true
          }
        end

        before do
          scheduler.schedule!
        end

        it 'should return the new job id' do
          expect(result_jid).to eq(new_jid)
        end

        it 'should schedule this work to run after the previous job' do
          expect(result_work).to include(expected_work)
        end

        context 'when a block is provided' do
          let(:worker_class_two) { Class.new(worker_class_base) {} }
          let(:block) do
            ->(scheduler) do
              scheduler.perform_after(worker_class_two)
            end
          end
          let(:result_item_two) { JSON.load(serialized_work_two) }
          let(:result_work_two) { Sidekiq.load_json(result_item_two['work']) if result_item_two }

          it 'should schedule new items after this one' do
            expect(result_work_two).not_to be_nil
          end
        end

        it 'should schedule to run in the default queue' do
          expect(result_queue).to eq('default')
        end

        it 'should schedule to run in the default resource' do
          expect(result_resource).to eq('default')
        end

        describe 'running middleware' do
          let(:middleware_args) { [] }
          let(:middleware_block) do
            ->(worker, msg, queue, &block) do
              result = block.call
              middleware_args.concat([worker, msg, queue, result])
              result
            end
          end

          it 'should run the client middleware on this item' do
            expect(middleware_args[0]).to eq(worker_class_name)
            expect(middleware_args[1]).to include(expected_work)
            expect(middleware_args[2]).to eq(expected_work['queue'])
            expect(middleware_args[3]).to include(expected_work)
          end

          context 'when invoking the middleware returns nil' do
            let(:middleware_block) { ->(_, _, _) { nil } }

            it 'should not schedule the work' do
              expect(result_work).to be_nil
            end
          end
        end

        context 'when the worker overrides the queue' do
          let(:worker_class) { Class.new(worker_class_base) { sidekiq_options queue: 'other queue' } }
          it { expect(result_queue).to eq('other queue') }
        end

        context 'when the worker overrides the resource' do
          let(:resource_name) { 'other resource' }
          let(:worker_class) { Class.new(worker_class_base) { sidekiq_options resource: 'other resource' } }
          it { expect(result_resource).to eq('other resource') }
        end

        context 'when the worker overrides the retry behaviour' do
          let(:worker_class) { Class.new(worker_class_base) { sidekiq_options retry: false } }
          it { expect(!!result_work['retry']).to eq(false) }
        end
      end

      describe '#perform_after' do
        let!(:result_jid) { scheduler.perform_after(worker_class, *args, &block) }
        it_behaves_like 'a method scheduling a worker'
      end

      describe '#perform_after_with_options' do
        let(:options) { {} }

        describe 'handling FutureWorkers' do
          let(:worker_class_base) do
            Class.new do
              include FutureWorker

              def self.to_s
                @name ||= Faker::Lorem.word
              end
            end
          end

          it 'should not support FutureWorkers' do
            expect { scheduler.perform_after_with_options(options, worker_class) }.to raise_error(ArgumentError, 'FutureWorkers cannot be scheduled for later!')
          end
        end

        describe 'work scheduling' do
          let!(:result_jid) { scheduler.perform_after_with_options(options, worker_class, *args, &block) }
          it_behaves_like 'a method scheduling a worker'

          describe 'using the options' do
            before { scheduler.schedule! }

            context 'when the queue is overridden' do
              let(:queue_name) { Faker::Lorem.word }
              let(:options) { {queue: queue_name} }

              it 'should schedule to run in the specified queue' do
                expect(result_queue).to eq(queue_name)
              end

            end

            context 'when the queue is overridden' do
              let(:resource_name) { Faker::Lorem.word }
              let(:options) { {resource: resource_name} }

              it 'should schedule to run in that resource' do
                expect(result_resource).to eq(resource_name)
              end
            end

            context 'when the job id is overridden' do
              let(:custom_jid) { SecureRandom.uuid }
              let(:options) { {job_id: custom_jid} }

              it 'should use that job id' do
                expect(result_work['jid']).to eq(custom_jid)
              end
            end

            context 'when a block is provided' do
              let(:worker_class_two) { Class.new(worker_class_base) {} }
              let(:block) do
                ->(scheduler) do
                  scheduler.perform_after(worker_class_two)
                end
              end
              let(:result_item_two) { JSON.load(serialized_work_two) }
              let(:result_work_two) { Sidekiq.load_json(result_item_two['work']) if result_item_two }

              it 'should schedule new items after this one' do
                expect(result_work_two).not_to be_nil
              end
            end
          end
        end
      end

    end
  end
end
