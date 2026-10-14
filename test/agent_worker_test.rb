# frozen_string_literal: true

require "test_helper"

# The Laravel Cloud receive path: messages come from the in-container agent
# over its unix socket, and outcomes are reported back to it — the worker never
# touches SQS on the receive side.
class AgentWorkerTest < Minitest::Test
  class Flaky < StandardError; end

  class AgentJob < ActiveJob::Base
    retry_on Flaky, wait: 5, attempts: 3, jitter: 0

    def perform(mode)
      raise Flaky if mode == "flaky"
      raise ArgumentError, "boom" if mode == "broken"
    end
  end

  def teardown
    @agent&.close
    super
  end

  def test_success_is_reported_as_processed
    run_agent(message_for("ok"))

    assert_equal [{ "messageId" => "m-1", "receiptHandle" => "r-1", "status" => "processed" }], @agent.results
    assert_equal [%w[started critical], %w[processed critical]], @events.queue_events
    assert_empty @sqs.sent
  end

  def test_a_retry_is_reported_as_released_with_the_backoff
    run_agent(message_for("flaky"))

    assert_equal [{ "messageId" => "m-1", "receiptHandle" => "r-1", "status" => "released", "delay" => 5 }], @agent.results
    assert_equal %w[started released], @events.queue_events.map(&:first)
    assert_empty @sqs.sent, "a retry must not push a duplicate message"
  end

  def test_the_agents_receive_count_drives_the_attempt_count
    run_agent(message_for("flaky", receive_count: 3))

    assert_equal "processed", @agent.results.sole["status"], "terminal failures are deleted via the processed outcome"
    assert_equal 3, @events.failed_jobs.sole["attempts"]
    assert_equal "critical", @events.failed_jobs.sole["queue"]
  end

  def test_a_terminal_failure_is_reported_as_processed_and_recorded
    run_agent(message_for("broken"))

    assert_equal "processed", @agent.results.sole["status"]
    assert_equal %w[started failed], @events.queue_events.map(&:first)
    assert_match(/boom/, @events.failed_jobs.sole["exception"])
  end

  def test_a_rejected_outcome_is_logged_and_the_worker_moves_on
    run_agent(message_for("ok"), message_for("ok", id: "m-2"), result_statuses: [422, 200])

    assert_equal %w[m-1 m-2], @agent.results.map { |result| result["messageId"] }
  end

  def test_a_wedged_agent_ends_the_worker
    @agent = AgentStub.new(next_status: 500)
    install_runtime(config: agent_config, agent: @agent)

    assert_raises(LaravelCloud::Queue::AgentClient::Unavailable) { work_off }
  end

  private

  def agent_config
    build_config(agent: { "enabled" => true })
  end

  def run_agent(*messages, result_statuses: [])
    @agent = AgentStub.new(messages: messages, result_statuses: result_statuses)
    install_runtime(config: agent_config, agent: @agent)

    work_off
  end

  def message_for(mode, id: "m-1", receive_count: 1)
    job = AgentJob.new(mode)

    {
      "messageId" => id,
      "receiptHandle" => id.sub("m", "r"),
      "body" => LaravelCloud::Queue::Message.encode(job),
      "queueUrl" => queue_url("critical"),
      "attributes" => { "ApproximateReceiveCount" => receive_count.to_s },
    }
  end
end
