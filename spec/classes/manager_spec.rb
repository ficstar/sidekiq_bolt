require 'rspec'

module Sidekiq
  describe Manager do

    subject { Manager.new(nil) }

    describe '#heartbeat' do
      let(:identity) { SecureRandom.uuid }
      let(:info) { {'identity' => identity, 'tag' => ''} }
      let(:serialized_info) { Sidekiq.dump_json(info) }

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
