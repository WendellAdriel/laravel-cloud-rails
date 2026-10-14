# frozen_string_literal: true

require "test_helper"

class FailedJobRetryTest < Minitest::Test
  Encrypter = LaravelCloud::Queue::LaravelEncrypter
  Retrier = LaravelCloud::Queue::FailedJobRetrier
  Response = Retrier::Response

  APP_KEY = "base64:#{Base64.strict_encode64('k' * 32)}".freeze
  URL = "https://cloud.laravel.com/failed-jobs?signature=abc"

  RUNS = []

  class FailingOnceJob < ActiveJob::Base
    def perform(value)
      RUNS << value
      raise ArgumentError, "first run fails" if RUNS.one?
    end
  end

  class RetryableJob < ActiveJob::Base
    def perform(value)
      RUNS << value
    end
  end

  class FifoJob < ActiveJob::Base
    queue_as "orders.fifo"
    def perform; end
  end

  # Stands in for Laravel Cloud's failed-jobs endpoint: serves scripted
  # responses per URL (the last one repeats) and records each request.
  class FakeCloud
    attr_reader :requests

    def initialize
      @responses = Hash.new { |hash, url| hash[url] = [] }
      @requests = []
    end

    def respond(url, *responses)
      @responses[url].concat(responses)
    end

    def call(url, headers)
      @requests << [url, headers]
      responses = @responses.fetch(url) { raise "unexpected request to #{url}" }
      response = responses.size > 1 ? responses.shift : responses.first
      raise response if response.is_a?(Exception)

      response
    end
  end

  def setup
    RUNS.clear
    install_runtime
    @cloud = FakeCloud.new
    @sleeps = []
  end

  # --- the encrypter ---------------------------------------------------------

  def test_encrypter_round_trips_cbc_and_gcm
    encrypter = Encrypter.new(APP_KEY)

    assert_equal "hello", encrypter.decrypt_string(encrypter.encrypt_string("hello"))
    assert_equal "hello", encrypter.decrypt_string(encrypter.encrypt_string("hello", gcm: true))
    assert_equal "hi", Encrypter.new("b" * 16).decrypt_string(Encrypter.new("b" * 16).encrypt_string("hi"))
  end

  def test_encrypter_names_its_cipher_as_laravel_does
    assert_equal "AES-256-CBC", Encrypter.new(APP_KEY).cipher
    assert_equal "AES-128-CBC", Encrypter.new("b" * 16).cipher
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

  # --- fetching --------------------------------------------------------------

  def test_requests_payload_version_1_with_the_cipher
    @cloud.respond(URL, page([failed_job("a")]))

    retrier.retry(URL)

    assert_equal [[URL, { "Cloud-Payload-Version" => "1", "Cloud-Encryption-Cipher" => "AES-256-CBC" }]], @cloud.requests
  end

  def test_follows_pages_until_there_is_no_next_link
    page2 = "#{URL}&page=2"
    @cloud.respond(URL, page([failed_job("a"), failed_job("b")], next_url: page2))
    @cloud.respond(page2, page([failed_job("c", queue: "critical")]))

    results = retrier.retry(URL)
    work_off(queues: %w[default critical])

    assert_equal %w[a b c], results.map(&:id)
    assert results.all?(&:retried?)
    assert_equal [URL, page2], @cloud.requests.map(&:first)
    assert_equal [queue_url("default"), queue_url("default"), queue_url("critical")], @sqs.sent.map { |sent| sent[:queue_url] }
    assert_equal [["a", "default"], ["b", "default"], ["c", "critical"]], retried_markers
    assert_equal %w[a b c], RUNS
  end

  def test_an_unversioned_response_is_a_single_failed_job
    @cloud.respond(URL, legacy(failed_job("solo")))

    assert_equal ["solo"], retrier.retry(URL).map(&:id)
    assert_equal [["solo", "default"]], retried_markers
  end

  def test_an_unsupported_payload_version_raises
    @cloud.respond(URL, Response.new(200, [["Cloud-Payload-Version", "2"]], encrypt({})))

    error = assert_raises(Retrier::Error) { retrier.retry(URL) }
    assert_match(/Unsupported payload version: 2/, error.message)
  end

  def test_an_empty_page_retries_nothing
    @cloud.respond(URL, page([]))

    assert_empty retrier.retry(URL)
    assert_empty @sqs.sent
  end

  def test_transient_failures_are_retried_with_laravels_backoff
    @cloud.respond(URL, Response.new(503, [], ""), Errno::ECONNRESET.new, page([failed_job("a")]))

    assert_equal ["a"], retrier.retry(URL).map(&:id)
    assert_equal [0.5, 1.0], @sleeps
  end

  def test_gives_up_after_ten_attempts
    @cloud.respond(URL, Response.new(500, [], ""))

    assert_raises(Retrier::Error) { retrier.retry(URL) }
    assert_equal 10, @cloud.requests.size
    assert_equal [0.5, 1, 2, 4, 8, 16, 32, 64, 128], @sleeps
  end

  def test_a_403_is_not_retried
    @cloud.respond(URL, Response.new(403, [], ""))

    error = assert_raises(Retrier::Error) { retrier.retry(URL) }
    assert_match(/HTTP 403/, error.message)
    assert_equal 1, @cloud.requests.size
  end

  # --- retrying --------------------------------------------------------------

  def test_a_real_failed_job_retried_from_the_dashboard_runs_again
    FailingOnceJob.perform_later("v")
    work_off
    failed = @events.failed_jobs.sole
    @cloud.respond(URL, page([failed.slice("id", "queue", "payload").merge("connection" => "cloud")]))

    retrier.retry(URL)
    work_off

    assert_equal %w[v v], RUNS
    assert_equal [[failed["id"], "default"]], retried_markers
    marker = @events.emitted.find { |event| event["retried_at"] }
    assert_equal %w[_cloud_event id queue retried_at], marker.keys, "the retried marker carries exactly Laravel's forget() fields"
    assert_match(/\A\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}\z/, marker["retried_at"])
    assert_equal %w[queued started failed queued started processed], @events.queue_events.map(&:first)
  end

  def test_a_fifo_failed_job_gets_default_fifo_attributes
    @cloud.respond(URL, page([{ "id" => "f", "queue" => "orders.fifo", "payload" => LaravelCloud::Queue::Message.encode(FifoJob.new) }]))

    retrier.retry(URL)

    sent = @sqs.sent.sole
    assert_equal queue_url("orders.fifo"), sent[:queue_url]
    assert_equal "orders.fifo", sent[:message_group_id]
    refute_nil sent[:message_deduplication_id]
  end

  def test_a_truncated_payload_is_skipped_and_left_in_the_dashboard
    truncated = JSON.generate("uuid" => "u", "displayName" => "BigJob", "data" => "{\"job_cl", "data_truncated" => true)
    @cloud.respond(URL, page([failed_job("a"), { "id" => "big", "queue" => "default", "payload" => truncated }, failed_job("c")]))

    results = retrier.retry(URL)

    assert_equal [true, false, true], results.map(&:retried?)
    assert_match(/too large to be kept whole/, results[1].error)
    assert_equal %w[a c], retried_markers.map(&:first), "a skipped job is not marked retried"
    assert_equal 2, @sqs.sent.size
  end

  def test_a_failed_push_stops_the_run_without_marking_that_job
    @sqs.client.stub_responses(:send_message, "ServiceUnavailable")
    @cloud.respond(URL, page([failed_job("a"), failed_job("b")]))

    assert_raises(Retrier::Error) { retrier.retry(URL) }
    assert_empty retried_markers
  end

  def test_requires_https_and_a_usable_app_key
    assert_raises(Retrier::Error) { retrier.retry("http://insecure") }
    assert_raises(Retrier::Error) { Retrier.new(runtime: @runtime, app_key: nil, fetcher: @cloud).retry(URL) }
    assert_raises(Retrier::Error) { Retrier.new(runtime: @runtime, app_key: "short", fetcher: @cloud).retry(URL) }
  end

  def test_rejects_failed_jobs_encrypted_with_another_key
    other = Encrypter.new("base64:#{Base64.strict_encode64('z' * 32)}").encrypt_string("{}")
    @cloud.respond(URL, Response.new(200, [], other))

    assert_raises(Retrier::Error) { retrier.retry(URL) }
  end

  private

  def retrier
    Retrier.new(runtime: @runtime, app_key: APP_KEY, fetcher: @cloud, sleeper: ->(seconds) { @sleeps << seconds })
  end

  def failed_job(id, queue: "default")
    { "id" => id, "queue" => queue, "connection" => "cloud", "payload" => LaravelCloud::Queue::Message.encode(RetryableJob.new(id)) }
  end

  def page(jobs, next_url: nil)
    body = { "data" => jobs, "links" => { "self" => URL, "next" => next_url } }

    Response.new(200, [["cloud-payload-version", "1"]], encrypt(body))
  end

  def legacy(job)
    Response.new(200, [], encrypt(job))
  end

  def encrypt(value)
    Encrypter.new(APP_KEY).encrypt_string(JSON.generate(value))
  end

  def retried_markers
    @events.emitted.select { |event| event["retried_at"] }.map { |event| [event["id"], event["queue"]] }
  end
end
