require "test_helper"
require "minitest/mock"
require "rails"
require_relative "../lib/laravel_cloud/websockets"

class WebsocketsRuntimeTest < Minitest::Test
  class RecordingBus
    attr_reader :receiver

    def subscribe(_channel, callback, _success)
      @receiver = callback
    end
  end

  def test_live_history_rollover_continues_without_accepting_obsolete_replay_positions
    state = { "epoch" => "old", "offset" => 7, "entries" => [{ "epoch" => "old", "offset" => 7, "time" => Time.now.to_i, "payload" => "before", "stream_id" => "stream" }] }
    broker = LaravelCloud::Websockets::Broker.allocate
    broker.define_singleton_method(:snapshot) { |_stream| state }
    bus = RecordingBus.new
    runtime = runtime_with(bus)
    stopped = false
    runtime.define_singleton_method(:stop) { stopped = true }
    messages = []
    entry = { "epoch" => "new", "offset" => 1, "time" => Time.now.to_i, "payload" => '{"text":"after expiry"}', "stream_id" => "stream" }

    LaravelCloud::Websockets.stub(:broker, broker) do
      runtime.subscribe("room", ->(message) { messages << message })
      second = entry.merge("offset" => 2, "payload" => '{"text":"second after expiry"}')
      state = { "epoch" => "new", "offset" => 2, "entries" => [entry, second] }
      assert_nil broker.read("room", { "epoch" => "old", "offset" => 7 })
      bus.receiver.call("cloud-stream", second)
      bus.receiver.call("cloud-stream", entry)
      bus.receiver.call("cloud-stream", entry.merge("epoch" => "old", "offset" => 7))
    end

    refute stopped
    assert_equal ['{"text":"after expiry"}', '{"text":"second after expiry"}'], messages
    assert_equal "new", messages.first.position.fetch("epoch")
  end

  def test_a_gap_beyond_current_epoch_retention_still_stops_the_transport
    state = { "epoch" => "same", "offset" => 7, "entries" => [{ "epoch" => "same", "offset" => 7, "time" => Time.now.to_i, "payload" => "before", "stream_id" => "stream" }] }
    broker = LaravelCloud::Websockets::Broker.allocate
    broker.define_singleton_method(:snapshot) { |_stream| state }
    bus = RecordingBus.new
    runtime = runtime_with(bus)
    stopped = false
    runtime.define_singleton_method(:stop) { stopped = true }
    messages = []
    entry = { "epoch" => "same", "offset" => 110, "time" => Time.now.to_i, "payload" => "missing history", "stream_id" => "stream" }

    LaravelCloud::Websockets.stub(:broker, broker) do
      runtime.subscribe("room", ->(message) { messages << message })
      state = { "epoch" => "same", "offset" => 110, "entries" => [entry] }
      bus.receiver.call("cloud-stream", entry)
    end

    assert stopped
    assert_empty messages
  end

  private

  def runtime_with(bus)
    LaravelCloud::Websockets::Runtime.allocate.tap do |runtime|
      runtime.instance_variable_set(:@bus, bus)
      runtime.instance_variable_set(:@lock, Mutex.new)
      runtime.instance_variable_set(:@listeners, {})
    end
  end
end
