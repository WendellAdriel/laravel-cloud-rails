# frozen_string_literal: true

require "test_helper"

# End to end over the SQS-direct path (no agent): jobs are enqueued through the
# real adapter and processed by the real worker, with Active Job's own
# retry_on / discard_on making every retry decision.
class WorkerTest < Minitest::Test
  class Flaky < StandardError; end
  class Gone < StandardError; end

  PERFORMED = []

  class SuccessfulJob < ActiveJob::Base
    def perform(value)
      PERFORMED << [value, executions]
    end
  end

  # Fails until its Nth execution, retrying in place.
  class FlakyJob < ActiveJob::Base
    retry_on Flaky, wait: 2, attempts: 3, jitter: 0

    def perform(succeed_on)
      PERFORMED << executions
      raise Flaky, "attempt #{executions}" if executions < succeed_on
    end
  end

  class LongBackoffJob < ActiveJob::Base
    retry_on Flaky, wait: 1.day, attempts: 2, jitter: 0

    def perform
      raise Flaky
    end
  end

  class MovingRetryJob < ActiveJob::Base
    retry_on Flaky, wait: 0, attempts: 2, queue: :slow, jitter: 0

    def perform
      PERFORMED << [queue_name, executions]
      raise Flaky if queue_name == "default"
    end
  end

  class DiscardedJob < ActiveJob::Base
    discard_on Gone

    def perform
      raise Gone
    end
  end

  class BrokenJob < ActiveJob::Base
    def perform(*)
      raise ArgumentError, "boom"
    end
  end

  def setup
    PERFORMED.clear
    install_runtime
  end

  def test_a_successful_job_is_performed_and_deleted
    SuccessfulJob.perform_later("hello")

    work_off

    assert_equal [["hello", 1]], PERFORMED
    assert_equal 1, @sqs.deleted.size
    assert_empty @sqs.pending
    assert_equal [%w[queued default], %w[started default], %w[processed default]], @events.queue_events
    assert_kind_of Integer, @events.emitted.last["duration_ms"]
  end

  def test_retry_on_releases_the_original_message_instead_of_pushing_a_copy
    FlakyJob.perform_later(3)

    work_off

    # Three deliveries of one message: executions are bridged from SQS's
    # receive count, so retry_on counts 1, 2, 3 and succeeds on the third.
    assert_equal [1, 2, 3], PERFORMED
    assert_equal 1, @sqs.sent.size, "a retry must not push a duplicate message"
    assert_equal [2, 2], @sqs.visibility_changes.map { |change| change[:visibility_timeout] }
    assert_empty @sqs.pending

    assert_equal %w[queued started released started released started processed], @events.queue_events.map(&:first)
  end

  def test_exhausting_retry_on_is_a_terminal_failure
    FlakyJob.perform_later(99)

    work_off

    assert_equal [1, 2, 3], PERFORMED
    assert_equal 2, @sqs.visibility_changes.size
    assert_empty @sqs.pending, "a terminally failed job is deleted"
    assert_equal %w[queued started released started released started failed], @events.queue_events.map(&:first)

    failed = @events.failed_jobs.sole
    assert_equal %w[_cloud_event id queue started_at attempts payload exception_preview job_name exception], failed.keys,
                 "the failed_job event must carry exactly Laravel's FailedJobProvider fields"
    assert_equal "WorkerTest::FlakyJob", failed["job_name"]
    assert_match(/\AWorkerTest::Flaky: attempt 3 in \/.+worker_test\.rb:\d+\z/, failed["exception_preview"])
    assert_equal "default", failed["queue"]
    assert_equal 3, failed["attempts"]
    assert_match(/WorkerTest::Flaky/, failed["exception"])
    assert_match(/attempt 3/, failed["exception"])
    assert_equal "WorkerTest::FlakyJob", JSON.parse(failed["payload"])["displayName"]
  end

  def test_retry_backoff_is_clamped_to_the_sqs_visibility_ceiling
    LongBackoffJob.perform_later

    work_off

    assert_equal [43_200], @sqs.visibility_changes.map { |change| change[:visibility_timeout] }
  end

  def test_a_retry_onto_another_queue_is_pushed_there_and_the_original_deleted
    MovingRetryJob.perform_later

    work_off
    work_off(queues: ["slow"])

    # Attempts are per message, so the moved job starts again at 1 there.
    assert_equal [["default", 1], ["slow", 1]], PERFORMED
    assert_equal [queue_url("default"), queue_url("slow")], @sqs.sent.map { |sent| sent[:queue_url] }
    assert_empty @sqs.visibility_changes
    assert_empty @sqs.pending
  end

  def test_a_re_pushed_message_starts_again_at_attempt_one
    # What Laravel Cloud pushes when a failed job is retried from the dashboard:
    # the stored body, verbatim — counters and all.
    payload = JSON.parse(LaravelCloud::Queue::Message.encode(SuccessfulJob.new("again")))
    payload["data"]["executions"] = 4
    @sqs.inject(queue_url("default"), JSON.generate(payload))

    work_off

    assert_equal [["again", 1]], PERFORMED
  end

  def test_an_unhandled_exception_is_terminal
    BrokenJob.perform_later

    work_off

    assert_empty @sqs.pending
    assert_empty @sqs.visibility_changes
    assert_equal 1, @events.failed_jobs.size
    assert_equal 1, @events.failed_jobs.sole["attempts"]
  end

  def test_a_discarded_job_is_processed
    DiscardedJob.perform_later

    work_off

    assert_empty @sqs.pending
    assert_equal %w[queued started processed], @events.queue_events.map(&:first)
  end

  def test_an_unknown_job_class_is_a_terminal_failure
    @sqs.inject(queue_url("default"), JSON.generate("uuid" => "x", "displayName" => "Nope", "data" => { "job_class" => "NoSuchJob", "job_id" => "x", "arguments" => [] }))

    work_off

    assert_empty @sqs.pending
    assert_equal 1, @events.failed_jobs.size
  end

  def test_a_poison_message_is_deleted_without_running
    @sqs.inject(queue_url("default"), "not json")

    work_off

    assert_empty @sqs.pending
    assert_empty @events.emitted
  end

  def test_a_failed_job_carries_its_message_body_whole
    BrokenJob.perform_later(["x" * 50_000])
    body = @sqs.pending.sole.body

    work_off

    # Exactly what sat on SQS, however large: Laravel Cloud stores it and
    # pushes it back verbatim to retry the job, as FailedJobProvider::log does.
    assert_equal body, @events.failed_jobs.sole["payload"]
  end

  def test_polls_the_requested_queues_in_turn
    SuccessfulJob.set(queue: "critical").perform_later("a")
    SuccessfulJob.perform_later("b")

    work_off(queues: %w[critical default])

    assert_equal [["a", 1], ["b", 1]], PERFORMED
  end
end
