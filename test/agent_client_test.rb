# frozen_string_literal: true

require "test_helper"

class AgentClientTest < Minitest::Test
  AgentClient = LaravelCloud::Queue::AgentClient

  def teardown
    @agent&.close
    super
  end

  def test_next_message_returns_the_payload_over_the_unix_socket
    @agent = AgentStub.new(messages: [{ "messageId" => "m-1", "body" => "{}" }])

    assert_equal({ "messageId" => "m-1", "body" => "{}" }, AgentClient.new(@agent.path).next_message)
    assert_equal ["GET /next"], @agent.requests
  end

  def test_next_message_is_nil_when_the_agent_has_nothing
    @agent = AgentStub.new

    assert_nil AgentClient.new(@agent.path).next_message
  end

  def test_next_message_raises_on_an_unexpected_status
    @agent = AgentStub.new(next_status: 500)

    assert_raises(AgentClient::Unavailable) { AgentClient.new(@agent.path).next_message }
  end

  def test_an_unreachable_socket_is_unavailable
    client = AgentClient.new(File.join(Dir.tmpdir, "missing-#{rand(1 << 32)}.sock"))

    assert_raises(AgentClient::Unavailable) { client.next_message }
    assert_raises(AgentClient::Unavailable) { client.report("m-1", "r-1", "processed") }
  end

  def test_report_posts_the_outcome
    @agent = AgentStub.new

    AgentClient.new(@agent.path).report("m-1", "r-1", "released", delay: 30)
    AgentClient.new(@agent.path).report("m-2", nil, "processed")

    assert_equal [
      { "messageId" => "m-1", "receiptHandle" => "r-1", "status" => "released", "delay" => 30 },
      { "messageId" => "m-2", "status" => "processed" },
    ], @agent.results
  end

  def test_report_distinguishes_a_rejection_from_a_wedged_agent
    @agent = AgentStub.new(result_statuses: [422, 503])
    client = AgentClient.new(@agent.path)

    assert_raises(AgentClient::Rejected) { client.report("m-1", "r-1", "processed") }
    assert_raises(AgentClient::Unavailable) { client.report("m-1", "r-1", "processed") }
  end
end
