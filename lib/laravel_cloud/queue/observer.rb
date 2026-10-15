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
    #   - "failed_job" the payload + exception for a terminal failure
    class Observer
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
          # The message body exactly as it sat on SQS: Laravel Cloud stores it
          # and pushes it back verbatim to retry the job.
          "payload" => delivery.body.to_s,
          "exception_preview" => preview(error),
          "job_name" => job_name(delivery.body),
          "exception" => describe(error),
        )

        emit_queue_event("failed", delivery.queue, finished_at, "duration_ms" => duration_ms(started_at, finished_at))
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
    end
  end
end
