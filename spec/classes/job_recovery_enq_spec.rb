require 'rspec'

module Sidekiq
  module Bolt
    describe JobRecoveryEnq do

      describe '#enqueue_jobs' do
        let(:process) { '0001' }
        let(:sweeper) { double(:sweeper, sweep: nil) }

        before do
          allow(ProcessSweeper).to receive(:new).with(process).and_return(sweeper)
          global_redis.sadd('bolt:processes', process)
        end

        it 'should sweep work for dead processes' do
          expect(sweeper).to receive(:sweep)
          subject.enqueue_jobs
        end

        context 'with multiple dead processes' do
          let(:process_two) { '0002' }
          let(:sweeper_two) { double(:sweeper, sweep: nil) }

          before do
            allow(ProcessSweeper).to receive(:new).with(process_two).and_return(sweeper_two)
            global_redis.sadd('bolt:processes', process_two)
          end

          it 'should sweep work for the first process' do
            expect(sweeper).to receive(:sweep)
            subject.enqueue_jobs
          end

          it 'should sweep work for the second process' do
            expect(sweeper_two).to receive(:sweep)
            subject.enqueue_jobs
          end
        end

        context 'when the process is still alive' do
          before { global_redis.set("bolt:processes:#{process}", SecureRandom.uuid) }

          it 'should not sweep' do
            expect(sweeper).not_to receive(:sweep)
            subject.enqueue_jobs
          end
        end

      end

    end
  end
end
