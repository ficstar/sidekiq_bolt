require 'rspec'

module Sidekiq
  describe Manager do

    subject { Manager.new(nil) }

    describe '#heartbeat' do
      let(:identity) { SecureRandom.uuid }
      let(:info) { {'identity' => identity, 'tag' => ''} }
      let(:serialized_info) { Sidekiq.dump_json(info) }

      describe 'heartbeat frequency' do
        it 'should call itself again in 5s' do
          expect_any_instance_of(Manager).to receive(:after).with(5) do |manager, _, &block|
            allow(subject).to receive(:after)
            expect(manager).to receive(:heartbeat).with(identity, info, serialized_info)
            block.call
          end
          subject.heartbeat(identity, info, serialized_info)
        end
      end

      describe 'the heartbeat' do
        before do
          allow(subject).to receive(:after)
          subject.heartbeat(identity, info, serialized_info)
        end

        it 'should add our process to the processes set' do
          expect(global_redis.smembers('bolt:processes')).to include(identity)
        end

        it 'should set the process info' do
          expect(global_redis.get("bolt:processes:#{identity}")).to eq(serialized_info)
        end

        it 'should set an expiration on the process info' do
          expect(global_redis.ttl("bolt:processes:#{identity}")).to within(5).of(60)
        end
      end
    end

  end
end
