# frozen_string_literal: true

require "test_helper"

class EventsTest < Minitest::Test
  def setup
    @dir = Dir.mktmpdir("events")
    @path = File.join(@dir, "cloud.sock")
  end

  def teardown
    FileUtils.rm_rf(@dir)
    super
  end

  def test_writes_newline_delimited_json_to_the_socket
    server = UNIXServer.new(@path)
    events = LaravelCloud::Events.new("unix://#{@path}")

    events.emit("_cloud_event" => "queue", "type" => "queued")
    events.emit_many([{ "n" => 1 }, { "n" => 2 }])

    connection = server.accept
    lines = 3.times.map { JSON.parse(connection.gets) }

    assert_equal [{ "_cloud_event" => "queue", "type" => "queued" }, { "n" => 1 }, { "n" => 2 }], lines
  ensure
    server&.close
  end

  def test_reconnects_after_the_collector_hangs_up
    server = UNIXServer.new(@path)
    events = LaravelCloud::Events.new(@path)

    events.emit("n" => 1)
    first = server.accept
    assert_equal({ "n" => 1 }, JSON.parse(first.gets))
    first.close

    events.emit("n" => 2)
    second = server.accept
    assert_equal({ "n" => 2 }, JSON.parse(second.gets))
  ensure
    server&.close
  end

  def test_never_raises_without_a_socket
    events = LaravelCloud::Events.new(@path)

    assert_nil events.emit("n" => 1)
  end
end
