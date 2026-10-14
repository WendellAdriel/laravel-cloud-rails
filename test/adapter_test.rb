# frozen_string_literal: true

require "test_helper"

class AdapterTest < Minitest::Test
  class ReportJob < ActiveJob::Base
    def perform(*); end
  end

  class CriticalJob < ActiveJob::Base
    queue_as :critical
    def perform; end
  end

  class OrderJob < ActiveJob::Base
    include LaravelCloud::Queue::MessageAttributes

    queue_as "orders.fifo"
    message_group_id { "customer-#{arguments.first}" }
    message_deduplication_id { "order-#{arguments.last}" }

    def perform(*); end
  end

  class FifoDefaultsJob < ActiveJob::Base
    queue_as "orders.fifo"
    def perform; end
  end

  class TenantJob < ActiveJob::Base
    include LaravelCloud::Queue::MessageAttributes

    message_group_id { "tenant-#{arguments.first}" }

    def perform(*); end
  end

  class DedupedStandardJob < ActiveJob::Base
    include LaravelCloud::Queue::MessageAttributes

    message_deduplication_id "nope"

    def perform; end
  end

  def setup
    install_runtime
  end

  def test_pushes_the_serialized_job_to_the_default_managed_queue
    job = ReportJob.perform_later(42)

    sent = @sqs.sent.sole
    body = JSON.parse(sent[:message_body])

    assert_equal queue_url("default"), sent[:queue_url]
    assert_equal job.job_id, body["uuid"]
    assert_equal "AdapterTest::ReportJob", body["displayName"]
    assert_equal job.job_id, body["data"]["job_id"]
    assert_equal [42], body["data"]["arguments"]
    assert_equal "msg-1", job.provider_job_id
    assert_equal [%w[queued default]], @events.queue_events
  end

  def test_active_jobs_default_queue_maps_to_the_environments_default_queue
    install_runtime(config: build_config(queue: "jobs"))

    ReportJob.perform_later

    assert_equal queue_url("jobs"), @sqs.sent.sole[:queue_url]
    assert_equal [%w[queued jobs]], @events.queue_events
  end

  def test_a_named_queue_targets_the_managed_queue_of_that_name
    CriticalJob.perform_later
    ReportJob.set(queue: "reports").perform_later

    assert_equal [queue_url("critical"), queue_url("reports")], @sqs.sent.map { |sent| sent[:queue_url] }
    assert_equal [%w[queued critical], %w[queued reports]], @events.queue_events
  end

  def test_a_wait_becomes_an_sqs_delivery_delay
    ReportJob.set(wait: 90).perform_later
    ReportJob.set(wait: 15.minutes).perform_later

    assert_equal [90, 900], @sqs.sent.map { |sent| sent[:delay_seconds] }
  end

  def test_a_wait_beyond_fifteen_minutes_fails_loudly
    error = assert_raises(LaravelCloud::Queue::InvalidDispatch) { ReportJob.set(wait: 16.minutes).perform_later }

    assert_match(/exceeds the SQS maximum of 900 seconds/, error.message)
    assert_empty @sqs.sent
  end

  def test_a_past_wait_until_is_sent_immediately
    ReportJob.set(wait_until: 1.minute.ago).perform_later

    refute @sqs.sent.sole.key?(:delay_seconds)
  end

  def test_fifo_queues_get_safe_default_group_and_deduplication_ids
    FifoDefaultsJob.perform_later
    FifoDefaultsJob.perform_later

    first, second = @sqs.sent

    assert_equal queue_url("orders.fifo"), first[:queue_url]
    assert_equal "orders.fifo", first[:message_group_id]
    refute_equal first[:message_deduplication_id], second[:message_deduplication_id]
  end

  def test_fifo_attributes_come_from_the_job
    OrderJob.perform_later(7, 99)

    sent = @sqs.sent.sole

    assert_equal "customer-7", sent[:message_group_id]
    assert_equal "order-99", sent[:message_deduplication_id]
  end

  def test_fifo_queues_reject_a_delay
    assert_raises(LaravelCloud::Queue::InvalidDispatch) { FifoDefaultsJob.set(wait: 5).perform_later }
  end

  def test_a_message_group_on_a_standard_queue_is_a_fair_queue_tenant_key
    TenantJob.set(wait: 10).perform_later(3)

    sent = @sqs.sent.sole

    assert_equal "tenant-3", sent[:message_group_id]
    assert_equal 10, sent[:delay_seconds]
    refute sent.key?(:message_deduplication_id)
  end

  def test_deduplication_on_a_standard_queue_fails_loudly
    assert_raises(LaravelCloud::Queue::InvalidDispatch) { DedupedStandardJob.perform_later }
  end

  def test_without_a_managed_queue_enqueueing_fails_loudly
    install_runtime(config: LaravelCloud::Queue::ManagedQueueConfig.from_env({}))

    error = assert_raises(LaravelCloud::Queue::NotConfigured) { ReportJob.perform_later }
    assert_match(/LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG/, error.message)
  end

  def test_sqs_failures_surface_as_active_job_enqueue_errors
    @sqs.client.stub_responses(:send_message, "ServiceUnavailable")
    job = ReportJob.new

    assert_equal false, job.enqueue
    assert_kind_of ActiveJob::EnqueueError, job.enqueue_error
    assert_empty @events.queue_events
  end
end
