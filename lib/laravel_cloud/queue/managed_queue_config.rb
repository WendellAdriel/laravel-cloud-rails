# frozen_string_literal: true

require "json"

module LaravelCloud
  module Queue
    # Parses the LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG blob injected by Laravel
    # Cloud and exposes the bits the adapter and worker need: the SQS queue URL,
    # region, credential mode, and the in-container agent's runtime socket.
    #
    # The injected JSON looks like:
    #
    #   {
    #     "driver": "cloud",
    #     "queue": "default",
    #     "connection": {
    #       "driver": "sqs",
    #       "prefix": "https://sqs.us-east-2.amazonaws.com/036991040854",
    #       "suffix": "-env-a21e326b-...",
    #       "queue": "default",
    #       "region": "us-east-2",
    #       "credentials": "ecs"
    #     },
    #     "agent": {
    #       "enabled": true,
    #       "socket": "/tmp/cloud-agent.sock"
    #     }
    #   }
    class ManagedQueueConfig
      DEFAULT_AGENT_SOCKET = "/tmp/cloud-agent.sock"
      FIFO_SUFFIX = ".fifo"

      attr_reader :queue_url, :region, :credentials, :queue, :agent_socket, :prefix, :suffix

      def self.from_env(env = ENV)
        from_json(env["LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG"], agent_socket: env["LARAVEL_CLOUD_AGENT_SOCKET"])
      end

      # Build the config from the raw env values. Both may be nil/empty (running
      # locally with no managed queue) — the result then simply reports itself
      # as not configured rather than raising, so the app still boots.
      def self.from_json(json, agent_socket: nil)
        data = parse(json)
        connection = data["connection"].is_a?(Hash) ? data["connection"] : {}

        # Laravel Cloud injects an "agent" slice describing the in-container
        # cloud-agent sidecar: whether it is serving this container ("enabled")
        # and the runtime socket to reach it on ("socket"). The flag decides the
        # receive path — see #agent_available?.
        agent = data["agent"].is_a?(Hash) ? data["agent"] : {}

        # The socket comes from the agent slice, falling back to the
        # LARAVEL_CLOUD_AGENT_SOCKET override and finally the conventional
        # default, so the agent always has a path to dial when enabled.
        socket = presence(agent["socket"]) || presence(agent_socket) || DEFAULT_AGENT_SOCKET

        new(
          prefix: connection["prefix"].to_s.sub(%r{/+\z}, ""),
          suffix: connection["suffix"].to_s,
          queue: (connection["queue"] || data["queue"] || "default").to_s,
          region: connection["region"]&.to_s,
          credentials: (connection["credentials"] || "default").to_s,
          agent_socket: socket,
          agent_enabled: agent["enabled"] == true,
        )
      end

      def self.parse(json)
        return {} if json.nil? || json.empty?

        decoded = JSON.parse(json)
        decoded.is_a?(Hash) ? decoded : {}
      rescue JSON::ParserError
        {}
      end
      private_class_method :parse

      def self.presence(value)
        value.is_a?(String) && !value.empty? ? value : nil
      end
      private_class_method :presence

      # Whether a logical queue name denotes a FIFO queue. The ".fifo" suffix is
      # the single marker SQS (and Laravel) use, so every FIFO branch routes
      # through this one predicate rather than re-testing the suffix inline.
      def self.fifo?(queue)
        queue.to_s.end_with?(FIFO_SUFFIX)
      end

      def initialize(prefix:, suffix:, queue:, region:, credentials:, agent_socket:, agent_enabled:)
        @prefix = prefix
        @suffix = suffix
        @queue = queue
        @region = region
        @credentials = credentials
        @agent_socket = agent_socket
        @agent_enabled = agent_enabled
        @queue_url = queue_url_for(queue)
      end

      # The full SQS queue URL for a logical managed-queue name — how a named
      # queue ("critical") is resolved on both the dispatch path and the
      # SQS-direct receive path. Nil when no prefix is configured (running
      # locally with no managed queue).
      #
      # Assembled the same way as Laravel's SqsQueue::suffixQueue. For a
      # standard queue that is {prefix}/{queue}{suffix}. A FIFO queue name must
      # keep ".fifo" at the very end, so the per-environment suffix goes *before*
      # it — {prefix}/{base}{suffix}.fifo — not after it, which would point at a
      # queue that doesn't exist. This is the inverse of #normalize_queue.
      def queue_url_for(queue)
        return nil if prefix.empty?

        if self.class.fifo?(queue)
          "#{prefix}/#{queue.delete_suffix(FIFO_SUFFIX)}#{suffix}#{FIFO_SUFFIX}"
        else
          "#{prefix}/#{queue}#{suffix}"
        end
      end

      # Reduce a full SQS queue URL (or name) back to its logical queue name by
      # stripping the configured prefix and suffix — the name the Laravel Cloud
      # dashboard groups metrics under. Mirrors Laravel's Cloud\Queue::
      # normalizeQueue, keeping a trailing ".fifo" in place for FIFO queues.
      def normalize_queue(queue)
        queue = queue.to_s
        queue = queue.delete_prefix("#{prefix}/") unless prefix.empty?

        return queue if suffix.empty?

        if self.class.fifo?(queue)
          base = queue.delete_suffix(FIFO_SUFFIX)
          base.end_with?(suffix) ? "#{base.delete_suffix(suffix)}#{FIFO_SUFFIX}" : queue
        else
          queue.delete_suffix(suffix)
        end
      end

      # Whether a managed queue has actually been provisioned (there is a queue
      # URL to push to). False when running outside Laravel Cloud.
      def configured?
        !queue_url.nil?
      end

      # Whether the in-container cloud-agent should be used for receiving.
      #
      # Driven by the "enabled" flag in the injected config rather than probing
      # the socket on disk: Laravel Cloud tells us authoritatively whether the
      # sidecar is serving this container, which is more reliable than racing a
      # socket that may appear or vanish. Outside Laravel Cloud the flag is
      # absent, so the worker receives straight from SQS.
      def agent_available?
        @agent_enabled
      end

      # Whether SQS credentials should come from the ECS container credential
      # provider rather than the SDK's default chain.
      def ecs_credentials?
        credentials == "ecs"
      end
    end
  end
end
