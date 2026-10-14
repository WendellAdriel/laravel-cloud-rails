# frozen_string_literal: true

require "test_helper"

# Laravel's `--tries` / `--backoff`, the delivery cap that comes with them, and
# the worker's other stop conditions.
class WorkerTriesTest < Minitest::Test
  class Flaky < StandardError; end

  PERFORMED = []

  class RaisingJob < ActiveJob::Base
    def perform
      PERFORMED << executions
      raise ArgumentError, "attempt #{executions}"
    end
  end

  class RetryingJob < ActiveJob::Base
    retry_on Flaky, wait: 0, attempts: 4, jitter: 0

    def perform
      PERFORMED << executions
      raise Flaky
    end
  end

  class UnlimitedJob < ActiveJob::Base
    retry_on Flaky, attempts: :unlimited
    def perform; end
  end

  class InheritedRetryJob < RetryingJob; end

  class QuickJob < ActiveJob::Base
    def perform
      PERFORMED << :ran
    end
  end

  class SlowJob < ActiveJob::Base
    def perform
      PERFORMED << :started
      sleep 5
      PERFORMED << :finished
    end
  end

  def setup
    PERFORMED.clear
    install_runtime
  end

  def test_retry_on_attempts_are_recorded_on_the_job_class
    assert_equal 4, RetryingJob.cloud_retry_attempts
    assert_equal 4, InheritedRetryJob.cloud_retry_attempts
    assert_equal Float::INFINITY, UnlimitedJob.cloud_retry_attempts
    assert_equal 0, QuickJob.cloud_retry_attempts
  end

  def test_tries_re_attempts_a_raising_job_after_the_backoff
    RaisingJob.perform_later

    work_off(tries: 3, backoff: 7)

    assert_equal [1, 2, 3], PERFORMED
    assert_equal [7, 7], @sqs.visibility_changes.map { |change| change[:visibility_timeout] }
    assert_equal %w[queued started released started released started failed], @events.queue_events.map(&:first)
    assert_equal 3, @events.failed_jobs.sole["attempts"]
  end

  def test_a_message_delivered_beyond_its_tries_fails_without_running
    RaisingJob.perform_later
    # A previous delivery crashed the worker before it reported anything.
    @sqs.pending.sole.receive_count = 1

    work_off

    assert_empty PERFORMED
    assert_empty @sqs.pending
    assert_match(/MaxAttemptsExceeded.*2 deliveries, limit 1/m, @events.failed_jobs.sole["exception"])
  end

  def test_retry_on_attempts_raise_the_delivery_cap_so_retries_are_not_cut_short
    RetryingJob.perform_later

    work_off

    assert_equal [1, 2, 3, 4], PERFORMED
    assert_match(/WorkerTriesTest::Flaky/, @events.failed_jobs.sole["exception"])
  end

  def test_a_disallowed_job_class_is_terminal_whatever_the_tries
    @runtime.settings.allowed_job_classes = ["WorkerTriesTest::QuickJob"]
    RaisingJob.perform_later
    QuickJob.perform_later

    work_off(tries: 5)

    assert_equal [:ran], PERFORMED
    assert_empty @sqs.visibility_changes
    assert_match(/not in the allowed job classes/, @events.failed_jobs.sole["exception"])
  end

  def test_a_name_that_is_not_an_active_job_is_terminal
    @sqs.inject(queue_url("default"), JSON.generate("data" => { "job_class" => "Kernel", "job_id" => "x" }))

    work_off(tries: 5)

    assert_empty @sqs.visibility_changes
    assert_match(/not an Active Job class/, @events.failed_jobs.sole["exception"])
  end

  def test_an_undefined_job_class_is_retried_for_a_rolling_deploy
    @sqs.inject(queue_url("default"), JSON.generate("data" => { "job_class" => "NotDeployedYetJob", "job_id" => "x" }))

    work_off(tries: 2, backoff: 3)

    assert_equal [3], @sqs.visibility_changes.map { |change| change[:visibility_timeout] }
    assert_match(/NotDeployedYetJob is not defined/, @events.failed_jobs.sole["exception"])
  end

  def test_on_failure_hears_about_terminal_failures_only
    failures = []
    @runtime.settings.on_failure = ->(error, job_data) { failures << [error.class, job_data["job_class"]] }
    RaisingJob.perform_later

    work_off(tries: 2)

    assert_equal [[ArgumentError, "WorkerTriesTest::RaisingJob"]], failures
  end

  def test_an_on_failure_handler_that_raises_does_not_stop_the_worker
    @runtime.settings.on_failure = ->(*) { raise "handler bug" }
    RaisingJob.perform_later
    QuickJob.perform_later

    work_off

    assert_equal [1, :ran], PERFORMED
  end

  def test_max_jobs_stops_the_worker
    3.times { QuickJob.perform_later }

    work_off(max_jobs: 2)

    assert_equal [:ran, :ran], PERFORMED
    assert_equal 1, @sqs.pending.size
  end

  def test_a_job_outliving_the_shutdown_timeout_is_abandoned_and_released
    SlowJob.perform_later
    worker = LaravelCloud::Queue::Worker.new(runtime: @runtime, shutdown_timeout: 0, stop_when_empty: true)

    Thread.new { sleep 0.1 until PERFORMED.include?(:started); worker.stop }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    worker.run

    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 3
    assert_equal [:started], PERFORMED
    assert_equal [0], @sqs.visibility_changes.map { |change| change[:visibility_timeout] }
    assert_equal %w[queued started released], @events.queue_events.map(&:first)
  end

  def test_a_stop_lets_the_running_job_finish_within_the_timeout
    SlowJob.perform_later
    QuickJob.perform_later
    worker = LaravelCloud::Queue::Worker.new(runtime: @runtime, shutdown_timeout: 30, stop_when_empty: true)

    Thread.new { sleep 0.1 until PERFORMED.include?(:started); worker.stop }
    worker.run

    assert_equal %i[started finished], PERFORMED, "the running job finishes; the next one is left queued"
    assert_equal 1, @sqs.pending.size
  end
end
