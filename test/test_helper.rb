# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "laravel_cloud"
require "minitest/autorun"
require "tmpdir"

ActiveJob::Base.logger = Logger.new(nil)

require_relative "support/fake_sqs"
require_relative "support/agent_stub"
require_relative "support/recording_events"

module LaravelCloudTestHelpers
  PREFIX = "https://sqs.us-east-2.amazonaws.com/036991040854"
  SUFFIX = "-env-a21e326b"

  def build_config(queue: "default", agent: nil)
    data = {
      "queue" => queue,
      "connection" => {
        "driver" => "sqs",
        "prefix" => PREFIX,
        "suffix" => SUFFIX,
        "queue" => queue,
        "region" => "us-east-2",
        "credentials" => "ecs",
      },
    }
    data["agent"] = agent if agent

    LaravelCloud::Queue::ManagedQueueConfig.from_json(JSON.generate(data))
  end

  def queue_url(name)
    build_config.queue_url_for(name)
  end

  # A runtime over a stateful in-memory SQS (and optionally an agent), with a
  # recording event sink, wired as the process-wide runtime the adapter uses.
  def install_runtime(config: build_config, agent: nil)
    @sqs = FakeSqs.new
    agent = LaravelCloud::Queue::AgentClient.new(agent.path) if agent.is_a?(AgentStub)
    @events = RecordingEvents.new
    @runtime = LaravelCloud::Queue::Runtime.new(config: config, sqs: @sqs.client, agent: agent, events: @events)
    LaravelCloud::Queue.runtime = @runtime
    ActiveJob::Base.queue_adapter = :cloud
  end

  def work_off(**options)
    LaravelCloud::Queue::Worker.new(runtime: @runtime, stop_when_empty: true, **options).run
  end

  def teardown
    LaravelCloud::Queue.runtime = nil
    super
  end
end

class Minitest::Test
  include LaravelCloudTestHelpers
end
