require 'rspec'

module Sidekiq
  module Bolt
    describe Scripts do

      let(:klass) do
        Class.new { include Scripts }
      end
      let(:instance) { klass.new }

      describe 'handling losing the script' do
        let(:keys) { %w(hello world) }
        let(:argv) { [rand(1..15).to_s] }
        let(:script) { 'return ARGV' }

        subject do
          instance.run_script(:scripty, script, keys, argv)
        end

        before do
          instance.run_script(:scripty, script, keys, argv) rescue nil
          global_redis.script(:flush)
        end

        it 'should not raise an error' do
          expect { subject }.not_to raise_error
        end

        it { is_expected.to eq(argv) }

        context 'when we raise a different error' do
          let(:script) { 'return joe' }

          it 'should raise that error' do
            expect { subject }.to raise_error
          end
        end
      end

    end
  end
end
