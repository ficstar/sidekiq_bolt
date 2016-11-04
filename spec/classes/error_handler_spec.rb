require 'rspec'

module Sidekiq
  module Bolt
    describe ErrorHandler do

      let(:error_handler_klass) do
        Class.new do
          include ErrorHandler

          def call(worker, job, error)

          end
        end
      end

      describe '.register' do
        let(:sidekiq_options) { {concurrency: 0} }
        let(:error_klass) { Faker::Lorem.sentence }

        before { error_handler_klass.register(error_klass) }

        describe 'the error handler' do
          subject { sidekiq_options[:error_handlers][error_klass] }

          it { is_expected.to be_a_kind_of(error_handler_klass) }
        end

        context 'when error handlers already exist' do
          let(:error_klass_two) { Faker::Lorem.sentence }
          let(:error_klass_two_handler) { ->() {} }
          let(:sidekiq_options) do
            {concurrency: 0, error_handlers: {error_klass_two => error_klass_two_handler}}
          end

          it 'should leave the existing handlers alone' do
            expect(sidekiq_options[:error_handlers]).to include(error_klass_two => error_klass_two_handler)
          end

          describe 'the error handler' do
            subject { sidekiq_options[:error_handlers][error_klass] }

            it { is_expected.to be_a_kind_of(error_handler_klass) }
          end
        end
      end

    end
  end
end
