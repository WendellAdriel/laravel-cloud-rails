# frozen_string_literal: true

require "json"
require "securerandom"

module LaravelCloud
  module Queue
    # Emits the dashboard's queue events, in the same format as Laravel's
    # Cloud\Queue and Cloud\FailedJobProvider:
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

      # Matches FailedJobProvider::log exactly — the same fields, and the
      # failed job before the "failed" event. The dashboard needs every field.
      def failed(delivery, error, started_at, finished_at)
        @events.emit(
          "_cloud_event" => "failed_job",
          "id" => uuid7(finished_at),
          "queue" => delivery.queue,
          "started_at" => format_time(started_at),
          "attempts" => delivery.receive_count,
          # Whole and unchanged: Cloud pushes it back as-is to retry the job.
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

      def emit_queue_event(type, queue, at, extra = {})
        @events.emit({
          "_cloud_event" => "queue",
          "timestamp" => format_time(at),
          "type" => type,
          "queue" => queue,
        }.merge(extra))
      end

      # A UUIDv7 for `time`, like Laravel's Str::uuid7($timestamp). It must be
      # v7: Cloud derives the failure time from it (UUIDv7ToDateTime).
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

      # "Class: message in file:line", as Laravel previews it.
      def preview(error)
        location = error.backtrace_locations&.first
        where = location ? " in #{utf8(location.absolute_path || location.path)}:#{location.lineno}" : ""
        message = utf8(error.message)

        line = message.empty? ? "#{error.class}#{where}" : "#{error.class}: #{message}#{where}"
        line[0, MAX_EXCEPTION_PREVIEW_CHARS]
      end

      def job_name(body)
        payload = JSON.parse(body.to_s)
        payload.is_a?(Hash) ? payload["displayName"].to_s : ""
      rescue JSON::ParserError
        ""
      end

      # "Class: message" then the backtrace, like PHP's (string) $exception.
      def describe(error)
        ["#{error.class}: #{utf8(error.message)}", *Array(error.backtrace).map { |line| utf8(line) }].join("\n")
      end

      # Exceptions can carry any bytes, and mixing encodings would raise here —
      # after the message is deleted, losing the failed job. Invalid bytes
      # become U+FFFD, like PHP's JSON_INVALID_UTF8_SUBSTITUTE.
      def utf8(value)
        value.to_s.dup.force_encoding(Encoding::UTF_8).scrub("\uFFFD")
      end
    end
  end
end
