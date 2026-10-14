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

      # Laravel caps the one-line exception preview at 1001 characters.
      MAX_EXCEPTION_PREVIEW_CHARS = 1001

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
      #
      # The "failed_job" fields are exactly those of Laravel's Cloud
      # FailedJobProvider::log — the dashboard's failed-job list is built from
      # them, so none may be left out (including "job_name" and
      # "exception_preview", which the dashboard lists failed jobs by).
      #
      # The order follows FailedJobProvider::log too: the failed job first,
      # then the "failed" lifecycle event (its finishProcessingJob call).
      def failed(delivery, error, started_at, finished_at)
        @events.emit(
          "_cloud_event" => "failed_job",
          "id" => uuid7(finished_at),
          "queue" => delivery.queue,
          "started_at" => format_time(started_at),
          # SQS's ApproximateReceiveCount, mirroring Laravel's SqsJob::attempts().
          "attempts" => delivery.receive_count,
          "payload" => trim_payload(delivery.body),
          "exception_preview" => preview(error),
          "job_name" => job_name(delivery.body),
          "exception" => truncate(describe(error), MAX_EXCEPTION_BYTES),
        )

        emit_queue_event("failed", delivery.queue, finished_at, "duration_ms" => duration_ms(started_at, finished_at))
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

      # A time-ordered UUIDv7 for `time`, as Laravel's Str::uuid7($timestamp)
      # gives a failed job's id: 48 bits of Unix milliseconds, then the version
      # and variant bits around random data (RFC 9562).
      def uuid7(time)
        milliseconds = (time.to_r * 1000).to_i
        bytes = [milliseconds >> 16, milliseconds & 0xFFFF].pack("Nn") + SecureRandom.random_bytes(10)

        bytes.setbyte(6, (bytes.getbyte(6) & 0x0F) | 0x70)
        bytes.setbyte(8, (bytes.getbyte(8) & 0x3F) | 0x80)

        hex = bytes.unpack1("H*")
        [hex[0, 8], hex[8, 4], hex[12, 4], hex[16, 4], hex[20, 12]].join("-")
      end

      def format_time(time)
        time.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")
      end

      def duration_ms(started_at, finished_at)
        [((finished_at - started_at) * 1000).round, 0].max
      end

      # Laravel's one-line preview: "Class: message in file:line", or
      # "Class in file:line" when there's no message.
      def preview(error)
        location = error.backtrace_locations&.first
        where = location ? " in #{utf8(location.absolute_path || location.path)}:#{location.lineno}" : ""
        message = utf8(error.message)

        line = message.empty? ? "#{error.class}#{where}" : "#{error.class}: #{message}#{where}"
        line[0, MAX_EXCEPTION_PREVIEW_CHARS]
      end

      # The payload's "displayName", as Laravel reads it; "" if there isn't one.
      def job_name(body)
        payload = JSON.parse(body.to_s)
        payload.is_a?(Hash) ? payload["displayName"].to_s : ""
      rescue JSON::ParserError
        ""
      end

      # The exception led by "Class: message", then its backtrace — the shape
      # of PHP's (string) $exception that Laravel and Symfony report, so the
      # dashboard's first line reads the same.
      def describe(error)
        ["#{error.class}: #{utf8(error.message)}", *Array(error.backtrace).map { |line| utf8(line) }].join("\n")
      end

      # A job's exception can carry any bytes in any encoding (a binary message,
      # a non-UTF-8 path). Reinterpret as UTF-8 with invalid bytes replaced by
      # U+FFFD — PHP's JSON_INVALID_UTF8_SUBSTITUTE — before anything is built
      # from it; mixing encodings would otherwise raise here, after the message
      # is already deleted, and the failed job would never be reported.
      def utf8(value)
        value.to_s.dup.force_encoding(Encoding::UTF_8).scrub("\uFFFD")
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
