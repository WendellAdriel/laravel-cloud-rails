# frozen_string_literal: true

require "test_helper"

# What actually reaches the Cloud log socket (LARAVEL_CLOUD_LOG_SOCKET) when a
# job fails, through the real LaravelCloud::Events emitter — the same lines
# Laravel's Cloud\Events + FailedJobProvider write for a failed Laravel job.
class CloudSocketTest < Minitest::Test
  class ExplodingJob < ActiveJob::Base
    def perform
      raise ArgumentError, "bad byte \xFF here".dup.force_encoding(Encoding::BINARY)
    end
  end

  def setup
    @dir = Dir.mktmpdir("cloud")
    @path = File.join(@dir, "cloud-init.sock")
    @server = UNIXServer.new(@path)
    @lines = Queue.new
    @reader = Thread.new do
      loop do
        connection = @server.accept
        Thread.new(connection) { |socket| socket.each_line { |line| @lines << JSON.parse(line) } }
      end
    end

    install_runtime
    @runtime = LaravelCloud::Queue::Runtime.new(
      config: build_config, sqs: @sqs.client, events: LaravelCloud::Events.new("unix://#{@path}"),
    )
    LaravelCloud::Queue.runtime = @runtime
  end

  def teardown
    @reader.kill
    @server.close
    FileUtils.rm_rf(@dir)
    super
  end

  def test_a_failed_job_reaches_the_socket_as_laravel_reports_it
    ExplodingJob.perform_later
    work_off

    events = Array.new(4) { @lines.pop(timeout: 2) }

    assert_equal [%w[queue queued], %w[queue started], ["failed_job", nil], %w[queue failed]],
                 events.map { |event| [event["_cloud_event"], event["type"]] },
                 "failed_job comes before the failed lifecycle event, as in FailedJobProvider::log"

    failed = events[2]
    assert_equal %w[_cloud_event id queue started_at attempts payload exception_preview job_name exception], failed.keys
    assert_match(/\A\h{8}-\h{4}-7\h{3}-[89ab]\h{3}-\h{12}\z/, failed["id"], "a UUIDv7, like Str::uuid7()")
    assert_equal "default", failed["queue"]
    assert_equal 1, failed["attempts"]
    assert_equal "CloudSocketTest::ExplodingJob", failed["job_name"]
    assert_match(/\AArgumentError: bad byte � here in /, failed["exception_preview"], "invalid UTF-8 is substituted, not dropped")
    assert_equal "CloudSocketTest::ExplodingJob", JSON.parse(failed["payload"])["displayName"]
  end

  def test_uuid7_ids_sort_by_failure_time
    observer = LaravelCloud::Queue::Observer.new(nil)
    earlier = observer.send(:uuid7, Time.at(1_700_000_000, 123, :millisecond))
    later = observer.send(:uuid7, Time.at(1_700_000_000, 124, :millisecond))

    assert_operator earlier, :<, later
    assert_equal format("%012x", 1_700_000_000_123), earlier.delete("-")[0, 12]
  end
end
