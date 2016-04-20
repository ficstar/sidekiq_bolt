require 'rspec'

module Sidekiq
  module Bolt
    describe WorkFuturePoller do

      before { WorkFuturePoller::WAITING.clear }

      describe 'polling' do
        let(:result) { Faker::Lorem.word }
        let(:jid) { SecureRandom.uuid }
        let!(:await) { WorkFuturePoller.await(jid) }
        let(:poller) { WorkFuturePoller.new }

        context 'with a single result' do
          let(:post_result) { [] }

          before do
            subject.on_success { |result| post_result << result }
            global_redis.set("worker:results:#{jid}", Sidekiq.dump_json(result)) if result
            poller.enqueue_jobs
          end

          describe '#await' do
            subject { await }

            it { is_expected.to be_a_kind_of(ThomasUtils::Observation) }
            its(:get) { is_expected.to eq(result) }

            context 'when #enqueue_jobs is called multiple time' do
              before { poller.enqueue_jobs }

              it 'should only fulfill the promise once' do
                expect(post_result).to eq([result])
              end
            end

            context 'when the result is not ready yet' do
              let(:result) { nil }

              it 'should not resolve the future' do
                expect(post_result).to be_empty
              end

              context 'when finally resolved' do
                let(:eventual_result) { Faker::Lorem.word }

                before do
                  global_redis.set("worker:results:#{jid}", Sidekiq.dump_json(eventual_result))
                  poller.enqueue_jobs
                end

                its(:get) { is_expected.to eq(eventual_result) }
              end
            end
          end

        end

        context 'with multiple results' do
          let(:result_two) { Faker::Lorem.word }
          let(:jid_two) { SecureRandom.uuid }
          let!(:await_two) { WorkFuturePoller.await(jid_two) }

          before do
            global_redis.set("worker:results:#{jid}", Sidekiq.dump_json(result))
            global_redis.set("worker:results:#{jid_two}", Sidekiq.dump_json(result_two))
            poller.enqueue_jobs
          end

          describe '#await' do
            subject { [await.get, await_two.get] }

            it { is_expected.to eq([result, result_two]) }
          end
        end

      end

    end
  end
end