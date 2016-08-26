require 'rspec'

module Sidekiq
  module Bolt
    describe FeedWorker do

      class MockFeedWorker
        include FeedWorker
      end

      class MockFeedWorkerTwo
        include FeedWorker
      end

      subject { MockFeedWorker }

      describe '.perform_async' do
        let(:args) { Faker::Lorem.words }

        shared_examples_for 'publishing a FeedWorker' do |channel|
          let(:message) { {'class' => subject.to_s, 'args' => args} }

          before do
            options = {}
            options[:channel] = channel unless channel == 'global'
            subject.sidekiq_options(options)
          end

          it 'should publish the message to redis' do
            expect_any_instance_of(Redis).to receive(:publish) do |_, result_channel, serialized_message|
              expect(result_channel).to eq("#{global_namespace}:#{channel}")

              item = Sidekiq.load_json(serialized_message)
              expect(item).to eq(message)
            end
            subject.perform_async(*args)
          end
        end

        it_behaves_like 'publishing a FeedWorker', 'global'
        it_behaves_like 'publishing a FeedWorker', Faker::Lorem.sentence

        context 'with a different worker' do
          subject { MockFeedWorkerTwo }

          it_behaves_like 'publishing a FeedWorker', 'global'
          it_behaves_like 'publishing a FeedWorker', Faker::Lorem.sentence
        end
      end

    end

    describe '#channel' do
      let(:channel) { Faker::Lorem.word }
      subject { MockFeedWorker.new }
      before { subject.channel = channel }
      its(:channel) { is_expected.to eq(channel) }
    end
  end
end
