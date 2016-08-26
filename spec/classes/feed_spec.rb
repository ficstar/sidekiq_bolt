require 'rspec'

module Sidekiq
  module Bolt
    describe Feed do

      class TestFeedWorker
        attr_accessor :channel

        class << self
          attr_accessor :results
        end

        def perform(*args)
          results << [channel, args]
        end

        def results
          self.class.results
        end
      end

      class MockSubscription < Struct.new(:channel, :serialized_message)
        def message
          yield channel, serialized_message
        end
      end

      let(:channel) { 'channel1' }
      let(:channels) { [channel] }
      let(:sidekiq_options) { {subscribed_channels: channels, concurrency: 0} }

      subject { Feed.new(sidekiq_options) }

      before { TestFeedWorker.results = [] }

      it { is_expected.to be_a_kind_of(Sidekiq::Util) }

      describe 'initialize' do
        let(:args) { Faker::Lorem.words }
        let(:message_identity) { nil }
        let(:process_identity) { SecureRandom.base64 }
        let(:work) { {'class' => TestFeedWorker.to_s, 'args' => args, 'pid' => message_identity} }
        let(:serialized_work) { Sidekiq.dump_json(work) }
        let(:subscription) { double(:subscription) }

        before do
          allow(subscription).to receive(:message)
          allow_any_instance_of(Redis).to receive(:subscribe) do |_, *channels, &block|
            channels.each { |channel| block[MockSubscription.new(channel, serialized_work)] }
          end
          allow_any_instance_of(Feed).to receive(:identity).and_return(process_identity)
          subject
        end

        shared_examples_for 'subscribing to a channel' do |channel|
          let(:expected_results) { [channel, args] }

          it 'performs the work on the specified class from the given channel' do
            expect(TestFeedWorker.results).to include(expected_results)
          end
        end

        it_behaves_like 'subscribing to a channel', 'channel1'

        context 'with no channels specified' do
          #let(:channel) { 'bolt:global' }
          let(:channels) { nil }

          it_behaves_like 'subscribing to a channel', 'bolt:global'
        end

        context 'when a process identity is specified' do
          let(:message_identity) { SecureRandom.base64 }

          it 'should not run anything that does not belong to it' do
            expect(TestFeedWorker.results).to be_empty
          end

          context 'when the message belongs to this process' do
            let(:message_identity) { process_identity }

            it_behaves_like 'subscribing to a channel', 'channel1'
          end
        end

        context 'with multiple channels' do
          let(:channel_two) { 'channel_two' }
          let(:channels) { [channel, channel_two] }

          it_behaves_like 'subscribing to a channel', 'channel1'
          it_behaves_like 'subscribing to a channel', 'channel_two'
        end

      end

    end
  end
end
