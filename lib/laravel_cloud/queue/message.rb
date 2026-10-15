# frozen_string_literal: true

require "json"

module LaravelCloud
  module Queue
    # The message body: Active Job's serialized job plus the "uuid" and
    # "displayName" keys of Laravel's payload, which the dashboard names
    # failed jobs by. Decoding only reads "data".
    #
    #   { "uuid": "<job_id>", "displayName": "ProcessReportJob", "data": { ...job.serialize... } }
    module Message
      class Undecodable < LaravelCloud::Error; end

      def self.encode(job)
        JSON.generate(
          "uuid" => job.job_id,
          "displayName" => job.class.name,
          "data" => job.serialize,
        )
      end

      # Decode a body into job data, with attempt counters taken from SQS.
      #
      # Retries release the original message (see Delivery), so its stored
      # counters never change and `retry_on` would retry forever. Instead,
      # delivery N has N-1 prior executions — per message, like Laravel's
      # SqsJob::attempts(), so a freshly pushed message (a dashboard retry, a
      # queue move) starts again at 1. `exception_executions` is cleared, which
      # makes `retry_on` count all deliveries rather than per exception.
      def self.decode(body, receive_count: 1)
        payload = JSON.parse(body.to_s)
        data = payload["data"] if payload.is_a?(Hash)

        unless data.is_a?(Hash) && data["job_class"].is_a?(String)
          raise Undecodable, "The managed queue message body is not in the expected format."
        end

        data.merge(
          "executions" => [receive_count.to_i, 1].max - 1,
          "exception_executions" => nil,
        )
      rescue JSON::ParserError
        raise Undecodable, "The managed queue message body is not valid JSON."
      end
    end
  end
end
