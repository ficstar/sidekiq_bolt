require 'rspec'

module Sidekiq
  module Bolt
    describe ErrorHandler do

      let(:error_handler_klass) do
        Class.new do
          include ErrorHandler
          define_method(:call) { |worker, job, error|}
        end
      end

      describe '.register' do
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

      describe '.invoke_handler' do
        let(:worker) { double(:worker) }
        let(:job) { double(:job) }
        let(:error_klass) { StandardError }
        let(:error_klass_two) { Interrupt }
        let(:error) { error_klass.new(Faker::Lorem.sentence) }

        before { error_handler_klass.register(error_klass) }

        it 'should call #call on the handler with the specified arguments' do
          expect_any_instance_of(error_handler_klass).to receive(:call).with(worker, job, error)
          ErrorHandler.invoke_handler(worker, job, error)
        end

        it 'should return true' do
          expect(ErrorHandler.invoke_handler(worker, job, error)).to eq(true)
        end

        context 'with multiple handlers' do
          let(:error_handler_klass_two) do
            Class.new do
              include ErrorHandler
              define_method(:call) { |worker, job, error|}
            end
          end
          let(:error) { error_klass_two.new(Faker::Lorem.sentence) }

          before { error_handler_klass_two.register(error_klass_two) }

          it 'should call #call on the correct handler' do
            expect_any_instance_of(error_handler_klass_two).to receive(:call).with(worker, job, error)
            ErrorHandler.invoke_handler(worker, job, error)
          end
        end

        context 'with a different error type' do
          let(:error) { error_klass_two.new(Faker::Lorem.sentence) }

          it 'should return false' do
            expect(ErrorHandler.invoke_handler(worker, job, error)).to eq(false)
          end

          context 'when the error can be handled by an ErrorHandler registered to a parent class' do
            let(:error_klass) { Exception }

            it 'should return true' do
              expect(ErrorHandler.invoke_handler(worker, job, error)).to eq(true)
            end
          end
        end
      end

    end
  end
end
