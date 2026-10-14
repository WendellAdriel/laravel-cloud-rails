# frozen_string_literal: true

require "json"

module LaravelCloud
  module Queue
    # The managed-queue message body: Active Job's own serialized job, wrapped
    # with the identifying keys of Laravel's queue payload.
    #
    #   { "uuid": "<job_id>", "displayName": "ProcessReportJob", "data": { ...job.serialize... } }
    #
    # "uuid" and "displayName" mirror Laravel's payload shape so the Laravel
    # Cloud dashboard can name a failed job. They are observability metadata
    # only; decoding reads "data", so the extra keys round-trip harmlessly
    # (including when the platform re-queues a failed job verbatim).
    module Message
      # The body isn't a managed-queue message this integration can run.
      class Undecodable < LaravelCloud::Error; end

      def self.encode(job)
        JSON.generate(
          "uuid" => job.job_id,
          "displayName" => job.class.name,
          "data" => job.serialize,
        )
      end

      # Decode a body into the job data Active Job executes, with its attempt
      # counters bridged from SQS's delivery count.
      #
      # Retries release the original message rather than pushing an updated
      # copy (see Delivery), so a redelivered message still carries the job data
      # from its first dispatch, and Active Job's own counters would read the
      # same value forever — retrying without end. SQS counts deliveries for
      # us, so we rebuild the counters from that: delivery #N has N-1 prior
      # executions on top of whatever the payload already carried (non-zero
      # only when the job was re-pushed, e.g. retried onto another queue).
      #
      # `exception_executions` — Active Job's per-exception retry counters — is
      # cleared: nothing could keep it up to date across a release. With it nil,
      # `retry_on` falls back to the overall `executions` count, so `attempts:`
      # counts every delivery of the job whichever exception failed it, the
      # same way Laravel's `$tries` counts SQS receives.
      def self.decode(body, receive_count: 1)
        payload = JSON.parse(body.to_s)
        data = payload["data"] if payload.is_a?(Hash)

        unless data.is_a?(Hash) && data["job_class"].is_a?(String)
          raise Undecodable, "The managed queue message body is not in the expected format."
        end

        data.merge(
          "executions" => data["executions"].to_i + [receive_count.to_i, 1].max - 1,
          "exception_executions" => nil,
        )
      rescue JSON::ParserError
        raise Undecodable, "The managed queue message body is not valid JSON."
      end
    end
  end
end
