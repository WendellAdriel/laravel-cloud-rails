# frozen_string_literal: true

require "test_helper"

class ManagedQueueConfigTest < Minitest::Test
  Config = LaravelCloud::Queue::ManagedQueueConfig

  def test_parses_the_injected_config
    config = build_config(agent: { "enabled" => true, "socket" => "/run/agent.sock" })

    assert config.configured?
    assert_equal "#{PREFIX}/default#{SUFFIX}", config.queue_url
    assert_equal "us-east-2", config.region
    assert config.ecs_credentials?
    assert config.agent_available?
    assert_equal "/run/agent.sock", config.agent_socket
  end

  def test_is_unconfigured_without_the_env_blob
    config = Config.from_env({})

    refute config.configured?
    assert_nil config.queue_url
    assert_nil config.queue_url_for("critical")
    refute config.agent_available?
    assert_equal "default", config.queue
  end

  def test_tolerates_malformed_json
    refute Config.from_json("{nope").configured?
  end

  def test_agent_is_off_unless_explicitly_enabled
    refute build_config.agent_available?
    refute build_config(agent: { "enabled" => "true" }).agent_available?
  end

  def test_agent_socket_falls_back_to_the_override_then_the_default
    assert_equal "/override.sock", Config.from_json("{}", agent_socket: "/override.sock").agent_socket
    assert_equal "/tmp/cloud-agent.sock", Config.from_json("{}", agent_socket: "").agent_socket
  end

  def test_fifo_queue_urls_keep_the_fifo_marker_last
    config = build_config

    assert_equal "#{PREFIX}/critical#{SUFFIX}", config.queue_url_for("critical")
    assert_equal "#{PREFIX}/orders#{SUFFIX}.fifo", config.queue_url_for("orders.fifo")
  end

  def test_normalize_queue_inverts_queue_url_for
    config = build_config

    %w[default critical orders.fifo].each do |name|
      assert_equal name, config.normalize_queue(config.queue_url_for(name))
    end
  end

  def test_trailing_prefix_slashes_are_trimmed
    config = Config.from_json(JSON.generate("connection" => { "prefix" => "https://sqs/1//", "queue" => "jobs" }))

    assert_equal "https://sqs/1/jobs", config.queue_url
  end
end
