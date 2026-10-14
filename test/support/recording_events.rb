# frozen_string_literal: true

# Stands in for LaravelCloud::Events, keeping what would have been written to
# the log socket. (Events itself is tested against a real socket.)
class RecordingEvents
  attr_reader :emitted

  def initialize
    @emitted = []
  end

  def emit(payload)
    @emitted << JSON.parse(JSON.generate(payload))
  end

  def queue_events
    emitted.select { |event| event["_cloud_event"] == "queue" }.map { |event| [event["type"], event["queue"]] }
  end

  def failed_jobs
    emitted.select { |event| event["_cloud_event"] == "failed_job" }
  end
end
