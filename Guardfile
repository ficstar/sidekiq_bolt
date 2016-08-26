guard :rspec, cmd: 'bundle exec rspec' do
  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^lib/(.+)\.rb$}) { |m| "spec/lib/#{m[1]}_spec.rb" }
  watch(%r{^lib/sidekiq/bolt\.rb$}) { 'spec' }
  watch(%r{^lib/sidekiq/bolt/(.+)\.rb}) { |m| "spec/classes/#{m[1]}_spec.rb" }
  watch(%r{^lib/sidekiq/bolt/(.+)/.+\.lua}) { |m| "spec/classes/#{m[1]}_spec.rb" }
  watch(%r{^lib/sidekiq/bolt/(.+)/(.+)\.rb}) { |m| "spec/classes/#{m[1]}/#{m[2]}_spec.rb" }
  watch('spec/spec_helper.rb') { 'spec' }
  watch(%r{^spec/shared_examples/scheduler\.rb}) { 'spec/classes/scheduler_spec.rb' }
  watch(%r{^spec/shared_examples/scheduler\.rb}) { 'spec/classes/child_scheduler_spec.rb' }
  watch(%r{^spec/helpers/(.+)\.rb}) { 'spec' }
  watch(%r{^spec/support/(.+)\.rb}) { 'spec' }
end

guard :bundler do
  require 'guard/bundler'
  require 'guard/bundler/verify'
  helper = Guard::Bundler::Verify.new

  files = ['Gemfile']
  files += Dir['*.gemspec'] if files.any? { |f| helper.uses_gemspec?(f) }

  # Assume files are symlinked from somewhere
  files.each { |file| watch(helper.real_path(file)) }
end
