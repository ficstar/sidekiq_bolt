require 'rspec'

module Sidekiq
  module Bolt
    module ServerMiddleware
      describe Statistics do

        let(:worker_class) { Class.new {} }
        let(:middleware) { Statistics.new }

        describe '#call' do
          let(:worker) { worker_class.new }
          let(:queue_name) { Faker::Lorem.word }
          let(:resource_name) { Faker::Lorem.word }
          let(:job_id) { SecureRandom.uuid }
          let(:parent_job_id) { SecureRandom.uuid }
          #noinspection RubyStringKeysInHashInspection
          let(:original_job) { {'queue' => queue_name, 'resource' => resource_name, 'jid' => job_id, 'pjid' => parent_job_id} }
          let(:job) { original_job.dup }
          let(:prev_resource_statistics) { {} }
          let(:prev_queue_statistics) { {} }
          let(:resource_statistics) do
            global_redis.hgetall("resource:stats:#{resource_name}")
          end
          let(:queue_statistics) do
            global_redis.hgetall("queue:stats:#{queue_name}")
          end

          before do
            global_redis.hmset("resource:stats:#{resource_name}", *prev_resource_statistics.to_a) unless prev_resource_statistics.empty?
            global_redis.hmset("queue:stats:#{queue_name}", *prev_queue_statistics.to_a) unless prev_queue_statistics.empty?
          end

          it_behaves_like 'a server middleware'

          context 'when an error happens' do
            let(:error) { StandardError.new(Faker::Lorem.sentence) }

            it 'should re-raise the error' do
              expect { subject.call(worker, job, nil) { raise error }.get }.to raise_error(error)
            end
          end

          describe 'statistics' do
            let(:block) { ->() {} }

            before { middleware.call(worker, job, nil, &block) rescue nil }

            shared_examples_for 'incrementing a statistic' do |stat, key|
              subject { public_send(stat) }
              it { is_expected.to include(key => '1') }

              context 'with a previous value' do
                let(:"prev_#{stat}") { {key => '13'} }
                it { is_expected.to include(key => '14') }
              end
            end

            shared_examples_for 'not incrementing a statistic' do |stat, key|
              subject { public_send(stat) }
              it { is_expected.not_to include(key) }

              context 'with a previous value' do
                let(:"prev_#{stat}") { {key => '13'} }
                it { is_expected.to include(key => '13') }
              end
            end

            describe 'a successful run' do
              it_behaves_like 'incrementing a statistic', :resource_statistics, 'successful'
              it_behaves_like 'incrementing a statistic', :queue_statistics, 'successful'
              it_behaves_like 'not incrementing a statistic', :resource_statistics, 'error'
              it_behaves_like 'not incrementing a statistic', :queue_statistics, 'error'
            end

            describe 'an erroneous run' do
              let(:block) { ->() { raise 'NOPE!' } }

              it_behaves_like 'not incrementing a statistic', :resource_statistics, 'successful'
              it_behaves_like 'not incrementing a statistic', :queue_statistics, 'successful'
              it_behaves_like 'incrementing a statistic', :resource_statistics, 'error'
              it_behaves_like 'incrementing a statistic', :queue_statistics, 'error'
            end
          end

        end

      end
    end
  end
end
