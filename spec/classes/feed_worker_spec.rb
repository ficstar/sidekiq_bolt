require 'rspec'

module Sidekiq
  module Bolt
    describe FeedWorker do

      let(:extra_options) { {} }
      let(:expected_channel) { nil }
      let(:worker_klass_name) { Faker::Lorem.words }
      let(:worker_klass) do
        klass_name = worker_klass_name
        Class.new do
          include FeedWorker
          @klass_name = klass_name

          def self.to_s
            @klass_name
          end
        end
      end

      subject { worker_klass }

      shared_examples_for 'publishing a FeedWorker' do |channel|
        let(:message) { {'class' => worker_klass_name, 'args' => args}.merge(extra_options) }

        before do
          options = {}
          options[:channel] = channel unless channel == 'global'
          subject.sidekiq_options(options)
        end

        it 'should publish the message to redis' do
          expect_any_instance_of(Redis).to receive(:publish) do |_, result_channel, serialized_message|
            expect(result_channel).to eq("#{global_namespace}:#{expected_channel || channel}")

            item = Sidekiq.load_json(serialized_message)
            expect(item).to eq(message)
          end
        end
      end

      describe '.perform_async' do
        let(:args) { Faker::Lorem.words }

        after { subject.perform_async(*args) }

        it_behaves_like 'publishing a FeedWorker', 'global'
        it_behaves_like 'publishing a FeedWorker', Faker::Lorem.sentence
      end

      describe '.perform_async' do
        let(:args) { Faker::Lorem.words }
        let(:perform_options) { {} }

        after { subject.perform_async_with_options(perform_options, *args) }

        it_behaves_like 'publishing a FeedWorker', 'global'
        it_behaves_like 'publishing a FeedWorker', Faker::Lorem.sentence

        context 'with an overridden channel' do
          let(:expected_channel) { Faker::Lorem.sentence }
          let(:perform_options) { {channel: expected_channel} }

          it_behaves_like 'publishing a FeedWorker', 'global'
        end

        context 'with a process identity filter specified' do
          let(:identity) { SecureRandom.base64 }
          let(:perform_options) { {process_identity: identity} }
          let(:extra_options) { {'pid' => identity} }

          it_behaves_like 'publishing a FeedWorker', 'global'
        end
      end

      describe '#channel' do
        let(:channel) { Faker::Lorem.word }
        subject { worker_klass.new }
        before { subject.channel = channel }
        its(:channel) { is_expected.to eq(channel) }
      end
    end
  end
end
