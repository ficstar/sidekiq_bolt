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

      let(:subscription_klass) do
        Struct.new(:channel, :serialized_message) do
          def message
            yield channel, serialized_message
          end
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
        let(:expected_error) { nil }
        let(:errors) { [] }

        before do
          allow(subscription).to receive(:message)
          allow_any_instance_of(Redis).to receive(:subscribe) do |_, *channels, &block|
            channels.each { |channel| block[subscription_klass.new(channel, serialized_work)] }
          end
          allow_any_instance_of(Feed).to receive(:identity).and_return(process_identity)
          allow_any_instance_of(TestFeedWorker).to receive(:perform).and_raise(expected_error) if expected_error
          begin
            subject
          rescue Exception => error
            errors << error
          end
        end

        shared_examples_for 'subscribing to a channel' do |channel|
          let(:expected_results) { [channel, args] }

          it 'performs the work on the specified class from the given channel' do
            expect(TestFeedWorker.results).to include(expected_results)
          end
        end

        it_behaves_like 'subscribing to a channel', 'channel1'

        context 'when working with the message causes a problem' do
          let(:error_message) { Faker::Lorem.sentence }
          let(:error_backtrace) { Faker::Lorem.sentences }
          let(:expected_error) { StandardError.new(error_message).tap { |error| error.set_backtrace(error_backtrace) } }

          it 'should not raise any errors' do
            expect(errors).to be_empty
          end

          it 'should log the error' do
            expect(global_error_log).to include("Error processing feed from channel '#{channel}': #{error_message}\n#{error_backtrace*"\n"}")
          end
        end

        context 'without a namespaced redis' do
          let(:sidekiq_redis_options) { {url: 'redis://redis.dev/13'} }
          it_behaves_like 'subscribing to a channel', 'channel1'
        end

        context 'with no channels specified' do
          let(:channels) { nil }
          it_behaves_like 'subscribing to a channel', 'global'
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
