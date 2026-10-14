# frozen_string_literal: true

require "test_helper"

class BatchEnqueueTest < Minitest::Test
  class BatchJob < ActiveJob::Base
    def perform(*); end
  end

  class FifoBatchJob < ActiveJob::Base
    queue_as "orders.fifo"
    def perform(*); end
  end

  def setup
    install_runtime
  end

  def test_perform_all_later_sends_batches_of_ten_per_queue
    jobs = Array.new(12) { |i| BatchJob.new(i) } + [BatchJob.new("c").set(queue: "critical")]

    ActiveJob.perform_all_later(jobs)

    assert_equal [10, 2, 1], @sqs.batches.map { |batch| batch[:entries].size }
    assert_equal [queue_url("default"), queue_url("default"), queue_url("critical")], @sqs.batches.map { |batch| batch[:queue_url] }
    assert jobs.all?(&:successfully_enqueued?)
    assert_equal 13, jobs.map(&:provider_job_id).uniq.size
    assert_equal 13, @events.queue_events.count { |type, _| type == "queued" }
  end

  def test_batches_also_split_on_total_size
    jobs = Array.new(3) { BatchJob.new("x" * 400_000) }

    ActiveJob.perform_all_later(jobs)

    assert_equal [2, 1], @sqs.batches.map { |batch| batch[:entries].size }
  end

  def test_scheduled_jobs_carry_their_delay
    ActiveJob.perform_all_later([BatchJob.new.set(wait: 60), BatchJob.new])

    assert_equal [60, nil], @sqs.batches.sole[:entries].map { |entry| entry[:delay_seconds] }
  end

  def test_fifo_jobs_get_default_group_and_deduplication_ids
    ActiveJob.perform_all_later([FifoBatchJob.new, FifoBatchJob.new])

    entries = @sqs.batches.sole[:entries]
    assert_equal ["orders.fifo"] * 2, entries.map { |entry| entry[:message_group_id] }
    assert_equal 2, entries.map { |entry| entry[:message_deduplication_id] }.uniq.size
  end

  def test_an_unsatisfiable_dispatch_raises_before_anything_is_sent
    jobs = [BatchJob.new, BatchJob.new.set(wait: 1.hour)]

    assert_raises(LaravelCloud::Queue::InvalidDispatch) { ActiveJob.perform_all_later(jobs) }
    assert_empty @sqs.batches
  end

  def test_a_rejected_entry_is_marked_on_its_job
    @sqs.reject_bodies_matching = /"reject-me"/
    good = BatchJob.new("fine")
    bad = BatchJob.new("reject-me")

    ActiveJob.perform_all_later([good, bad])

    assert good.successfully_enqueued?
    refute bad.successfully_enqueued?
    assert_kind_of ActiveJob::EnqueueError, bad.enqueue_error
    assert_match(/InvalidParameterValue/, bad.enqueue_error.message)
    assert_equal 1, @events.queue_events.size
  end

  def test_a_failed_batch_call_marks_every_job_in_it
    @sqs.client.stub_responses(:send_message_batch, "ServiceUnavailable")
    jobs = [BatchJob.new, BatchJob.new]

    ActiveJob.perform_all_later(jobs)

    assert jobs.none?(&:successfully_enqueued?)
    assert jobs.all? { |job| job.enqueue_error.is_a?(ActiveJob::EnqueueError) }
  end
end
