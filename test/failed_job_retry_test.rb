# frozen_string_literal: true

require "test_helper"

class FailedJobRetryTest < Minitest::Test
  Encrypter = LaravelCloud::Queue::LaravelEncrypter
  Retrier = LaravelCloud::Queue::FailedJobRetrier

  APP_KEY = "base64:#{Base64.strict_encode64('k' * 32)}".freeze

  RUNS = []

  class FailingOnceJob < ActiveJob::Base
    def perform(value)
      RUNS << value
      raise ArgumentError, "first run fails" if RUNS.one?
    end
  end

  class FifoJob < ActiveJob::Base
    queue_as "orders.fifo"
    def perform; end
  end

  def setup
    RUNS.clear
    install_runtime
  end

  # --- the encrypter ---------------------------------------------------------

  def test_encrypter_round_trips_cbc_and_gcm
    encrypter = Encrypter.new(APP_KEY)

    assert_equal "hello", encrypter.decrypt_string(encrypter.encrypt_string("hello"))
    assert_equal "hello", encrypter.decrypt_string(encrypter.encrypt_string("hello", gcm: true))
    assert_equal "hi", Encrypter.new("b" * 16).decrypt_string(Encrypter.new("b" * 16).encrypt_string("hi"))
  end

  def test_encrypter_rejects_another_key_and_tampering
    payload = Encrypter.new(APP_KEY).encrypt_string("secret")
    other = Encrypter.new("base64:#{Base64.strict_encode64('z' * 32)}")

    assert_raises(Encrypter::DecryptError) { other.decrypt_string(payload) }

    data = JSON.parse(Base64.decode64(payload))
    data["value"] = Base64.strict_encode64("tampered")
    assert_raises(Encrypter::DecryptError) { Encrypter.new(APP_KEY).decrypt_string(Base64.strict_encode64(JSON.generate(data))) }
    assert_raises(Encrypter::DecryptError) { Encrypter.new(APP_KEY).decrypt_string("garbage") }
  end

  def test_encrypter_requires_a_valid_key
    assert_raises(ArgumentError) { Encrypter.new("too-short") }
  end

  # --- retrying --------------------------------------------------------------

  def test_a_failed_job_retried_from_the_dashboard_runs_again
    FailingOnceJob.perform_later("v")
    work_off
    failed_job = @events.failed_jobs.sole

    retried = retrier_for(failed_job).retry("https://cloud.laravel.com/failed/1?signature=abc")
    work_off

    assert_equal %w[v v], RUNS
    assert_equal failed_job["id"], retried["id"]
    assert_equal queue_url("default"), @sqs.sent.last[:queue_url]

    marker = @events.emitted.find { |event| event["_cloud_event"] == "failed_job" && event["retried_at"] }
    assert_equal({ "id" => failed_job["id"], "queue" => "default" }, marker.except("_cloud_event", "retried_at"))
    assert_match(/\A\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}\z/, marker["retried_at"])
    assert_equal %w[queued started failed queued started processed], @events.queue_events.map(&:first)
  end

  def test_a_fifo_failed_job_gets_default_fifo_attributes
    failed_job = { "id" => "f-1", "queue" => "orders.fifo", "payload" => LaravelCloud::Queue::Message.encode(FifoJob.new) }

    retrier_for(failed_job).retry("https://x")

    sent = @sqs.sent.sole
    assert_equal queue_url("orders.fifo"), sent[:queue_url]
    assert_equal "orders.fifo", sent[:message_group_id]
    refute_nil sent[:message_deduplication_id]
  end

  def test_a_truncated_payload_cannot_be_retried
    payload = JSON.generate("uuid" => "u", "displayName" => "BigJob", "data" => "{\"job_cl", "data_truncated" => true)
    failed_job = { "id" => "f-2", "queue" => "default", "payload" => payload }

    error = assert_raises(Retrier::Error) { retrier_for(failed_job).retry("https://x") }

    assert_match(/too large to be kept whole/, error.message)
    assert_empty @sqs.sent
  end

  def test_requires_https_and_an_app_key
    assert_raises(Retrier::Error) { retrier_for({}).retry("http://insecure") }
    assert_raises(Retrier::Error) { Retrier.new(runtime: @runtime, app_key: nil, fetcher: ->(_) { "" }).retry("https://x") }
  end

  def test_rejects_a_failed_job_encrypted_with_another_key
    body = Encrypter.new("base64:#{Base64.strict_encode64('z' * 32)}").encrypt_string("{}")
    retrier = Retrier.new(runtime: @runtime, app_key: APP_KEY, fetcher: ->(_) { body })

    assert_raises(Retrier::Error) { retrier.retry("https://x") }
  end

  private

  def retrier_for(failed_job)
    body = Encrypter.new(APP_KEY).encrypt_string(JSON.generate(failed_job))

    Retrier.new(runtime: @runtime, app_key: APP_KEY, fetcher: ->(_url) { body })
  end
end
