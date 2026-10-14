# frozen_string_literal: true

require "test_helper"
require "laravel_cloud/cli"
require "stringio"

class CLITest < Minitest::Test
  PERFORMED = []

  class CliJob < ActiveJob::Base
    def perform(value)
      PERFORMED << [value, queue_name]
    end
  end

  def setup
    PERFORMED.clear
    install_runtime
    @out = StringIO.new
    @err = StringIO.new
  end

  def test_queue_work_runs_the_worker_with_its_options
    CliJob.set(queue: "b").perform_later(1)
    CliJob.set(queue: "a").perform_later(2)

    assert_equal 0, cli("queue:work", "--stop-when-empty", "-q", "a,b", "--tries", "2")
    assert_equal [[2, "a"], [1, "b"]], PERFORMED
  end

  def test_expected_failures_exit_non_zero_with_the_reason
    install_runtime(config: LaravelCloud::Queue::ManagedQueueConfig.from_env({}))

    assert_equal 1, cli("queue:work", "--stop-when-empty")
    assert_match(/LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG is missing/, @err.string)
  end

  def test_queue_retry_needs_a_url
    assert_equal 1, cli("queue:retry")
    assert_match(/Usage: laravel-cloud queue:retry/, @err.string)
  end

  def test_unknown_commands_and_options_are_rejected
    assert_equal 1, cli("queue:nope")
    assert_equal 1, cli("queue:work", "--bogus")
    assert_equal 0, cli
    assert_match(/queue:work/, @err.string)
  end

  def test_booting_outside_an_app_fails_clearly
    Dir.mktmpdir do |dir|
      Dir.chdir(dir) do
        assert_equal 1, LaravelCloud::CLI.new(%w[queue:work], out: @out, err: @err).run
      end
    end

    assert_match(/No Rails application found/, @err.string)
  end

  private

  def cli(*argv)
    LaravelCloud::CLI.new(argv, out: @out, err: @err, boot: false).run
  end
end
