# frozen_string_literal: true

require "json"
require "securerandom"

module LaravelCloud
  module Queue
    # Emits Laravel Cloud queue observability events for managed-queue jobs, so
    # the Cloud dashboard shows the same throughput, latency and failure metrics
    # for a Rails app as it does for a Laravel one.
    #
    # The wire protocol matches Laravel's Foundation\Cloud\Queue +
    # FailedJobProvider:
    #
    #   - "queued"     when a job is pushed onto a managed queue
    #   - "started"    when a worker picks a job up
    #   - "processed"  when the job succeeds
    #   - "released"   when the job fails but will be retried
    #   - "failed"     when the job fails terminally
    #   - "failed_job" the payload + exception for a terminal failure, and
    #                  again with "retried_at" when it's retried from the dashboard
    class Observer
      # Byte budgets for the two large fields of a "failed_job" event.
      #
      # TODO: REMOVE THIS TRIMMING once the platform's logging collector
      # reassembles CRI partial log lines. It is a temporary workaround — the
      # caps, #trim_payload/#truncate and their call sites should all go, and
      # the full payload/exception be emitted again.
      #
      # Laravel Cloud's log pipeline ships container output through a Fluent Bit
      # collector. containerd splits any line over 16 KiB into CRI partial
      # records, which the collector doesn't currently stitch back together
      # (no `multiline.parser cri`), so an oversized event arrives as truncated,
      # invalid JSON and never reaches the dashboard. Lifecycle events are tiny;
      # only "failed_job" (payload + backtrace) crosses the line, so it is kept
      # comfortably under 16 KiB here. The real fix belongs in the collector.
      MAX_EXCEPTION_BYTES = 4000
      MAX_PAYLOAD_DATA_BYTES = 2000

      def initialize(events)
        @events = events
      end

      def queued(queue)
        emit_queue_event("queued", queue, now)
      end

      def started(queue, at)
        emit_queue_event("started", queue, at)
      end

      def processed(queue, started_at, finished_at)
        emit_queue_event("processed", queue, finished_at, "duration_ms" => duration_ms(started_at, finished_at))
      end

      def released(queue, started_at, finished_at)
        emit_queue_event("released", queue, finished_at, "duration_ms" => duration_ms(started_at, finished_at))
      end

      # A terminal failure is reported twice: as a "failed" lifecycle event, and
      # as a failed job carrying the payload and exception so it can be
      # inspected from the Laravel Cloud dashboard.
      def failed(delivery, error, started_at, finished_at)
        emit_queue_event("failed", delivery.queue, finished_at, "duration_ms" => duration_ms(started_at, finished_at))

        @events.emit(
          "_cloud_event" => "failed_job",
          "id" => SecureRandom.uuid,
          "queue" => delivery.queue,
          "started_at" => format_time(started_at),
          # SQS's ApproximateReceiveCount, mirroring Laravel's SqsJob::attempts().
          "attempts" => delivery.receive_count,
          "payload" => trim_payload(delivery.body),
          "exception" => truncate(describe(error), MAX_EXCEPTION_BYTES),
        )
      end

      # A failed job retried from the dashboard: the same "failed_job" channel,
      # marked with "retried_at" so the dashboard records it as retried.
      def retried(id, queue)
        @events.emit(
          "_cloud_event" => "failed_job",
          "id" => id,
          "queue" => queue,
          "retried_at" => format_time(now),
        )
      end

      def now
        Time.now.utc
      end

      private

      # Every lifecycle emit funnels through here: the "_cloud_event"/"queue"
      # channel and the "Y-m-d H:i:s.u" timestamp are the dashboard's contract.
      def emit_queue_event(type, queue, at, extra = {})
        @events.emit({
          "_cloud_event" => "queue",
          "timestamp" => format_time(at),
          "type" => type,
          "queue" => queue,
        }.merge(extra))
      end

      def format_time(time)
        time.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")
      end

      def duration_ms(started_at, finished_at)
        [((finished_at - started_at) * 1000).round, 0].max
      end

      # The exception led by "Class: message", then its backtrace — the shape
      # of PHP's (string) $exception that Laravel and Symfony report, so the
      # dashboard's first line reads the same. Scrubbed to valid UTF-8 so it
      # can always be JSON-encoded.
      def describe(error)
        ["#{error.class}: #{error.message}", *error.backtrace].join("\n").scrub
      end

      # Shrink the failed-job payload to keep the event under the collector's
      # line limit (see the constants). The identifying "uuid"/"displayName"
      # survive so the dashboard can still name the job; the bulky job data is
      # truncated. A truncated payload can't reconstruct the job, an accepted
      # trade-off of the workaround.
      def trim_payload(body)
        payload = JSON.parse(body.to_s)
        return truncate(body.to_s, MAX_PAYLOAD_DATA_BYTES) unless payload.is_a?(Hash)

        data = JSON.generate(payload["data"])
        return body if data.bytesize <= MAX_PAYLOAD_DATA_BYTES

        JSON.generate(
          "uuid" => payload["uuid"],
          "displayName" => payload["displayName"],
          "data" => data.byteslice(0, MAX_PAYLOAD_DATA_BYTES).scrub(""),
          "data_truncated" => true,
        )
      rescue JSON::ParserError, JSON::GeneratorError
        truncate(body.to_s.scrub, MAX_PAYLOAD_DATA_BYTES)
      end

      # Cut a string to at most `max_bytes`, appending an elision marker when
      # anything was removed, without splitting a UTF-8 sequence.
      def truncate(value, max_bytes)
        return value if value.bytesize <= max_bytes

        "#{value.byteslice(0, max_bytes).scrub('')}\n…[truncated #{value.bytesize - max_bytes} bytes — see logging-collector CRI note]"
      end
    end
  end
end
