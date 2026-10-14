# frozen_string_literal: true

require "json"
require "net/http"
require "socket"

module LaravelCloud
  module Queue
    # Talks to the in-container cloud-agent over its unix runtime socket.
    #
    # The agent long-polls a single SQS queue on behalf of one consumer, holds
    # the received message (heartbeating its visibility), and hands it over via
    # GET /next. The consumer reports the terminal outcome via POST /result, at
    # which point the agent performs the actual SQS delete or visibility change.
    # The app container never touches SQS on the receive path.
    class AgentClient
      # The agent cannot serve work: its socket is unreachable or it answered
      # with a server error. A wedged agent only recovers on restart, so the
      # worker lets this escape and exits for the container to be restarted.
      # The job is never lost — a dead agent stops heartbeating, so SQS
      # redelivers once the visibility timeout lapses.
      class Unavailable < LaravelCloud::Error; end

      # The agent refused a reported outcome (HTTP 4xx). It is the authority on
      # what is valid for that one message, so the worker reports this and moves
      # on rather than restarting.
      class Rejected < LaravelCloud::Error; end

      # Outlast the agent's ~55s poll cycle so a 204 is a deliberate "nothing
      # yet", never our own timeout cutting a poll short.
      NEXT_TIMEOUT = 65
      RESULT_TIMEOUT = 10
      RESULT_ATTEMPTS = 3

      # Failures dialing the socket, before the agent has seen the request — the
      # only ones safe to retry, since a report is never delivered twice.
      CONNECT_ERRORS = [Errno::ENOENT, Errno::ECONNREFUSED].freeze

      # Failures once the connection is up, where the agent may already have
      # acted on the request.
      TRANSPORT_ERRORS = [
        Errno::ECONNRESET, Errno::EPIPE, EOFError, IOError,
        Net::ReadTimeout, Net::WriteTimeout, Net::OpenTimeout, Net::HTTPBadResponse,
      ].freeze

      def initialize(socket_path)
        @socket_path = socket_path
      end

      # Long-poll the agent for the next message.
      #
      # Returns the decoded payload (messageId, receiptHandle, body, attributes,
      # queueUrl), or nil when the agent has nothing (HTTP 204). Anything else
      # means the agent cannot serve work and raises Unavailable.
      def next_message
        response = request(Net::HTTP::Get.new("/next"), timeout: NEXT_TIMEOUT)

        return nil if response.code == "204"

        raise Unavailable, "The Laravel Cloud agent returned HTTP #{response.code} from GET /next." unless response.code == "200"

        data = JSON.parse(response.body.to_s)
        raise Unavailable, "The Laravel Cloud agent returned a non-object body from GET /next." unless data.is_a?(Hash)

        data
      rescue JSON::ParserError
        raise Unavailable, "The Laravel Cloud agent returned a malformed body from GET /next."
      end

      # Report a message's terminal outcome so the agent can stop heartbeating
      # and perform the SQS operation. "processed" deletes the message;
      # "released" resets its visibility (after `delay` seconds) so SQS
      # redelivers it. There is no "failed" outcome: a terminal failure is
      # reported as "processed" and its failure signal is emitted separately as
      # an observability event.
      #
      # Transient socket hiccups are retried; a persistently unreachable socket
      # or a server error raises Unavailable, a client error raises Rejected.
      def report(message_id, receipt_handle, status, delay: nil)
        payload = {
          "messageId" => message_id,
          "receiptHandle" => receipt_handle,
          "status" => status,
          "delay" => delay,
        }.compact

        attempts = 0

        begin
          attempts += 1

          post = Net::HTTP::Post.new("/result", "Content-Type" => "application/json")
          post.body = JSON.generate(payload)

          response = request(post, timeout: RESULT_TIMEOUT, retry_connect: attempts < RESULT_ATTEMPTS)
        rescue RetryableConnectionError
          sleep 0.1
          retry
        end

        code = response.code.to_i

        raise Unavailable, "The Laravel Cloud agent returned HTTP #{code} from POST /result." if code >= 500
        raise Rejected, "The Laravel Cloud agent rejected the result with HTTP #{code}." if code >= 400

        nil
      end

      private

      class RetryableConnectionError < StandardError; end
      private_constant :RetryableConnectionError

      def request(request, timeout:, retry_connect: false)
        http = UnixHTTP.new("localhost")
        http.socket_path = @socket_path
        http.open_timeout = timeout
        http.read_timeout = timeout
        http.write_timeout = timeout
        # A single attempt: a long-poll must never be silently re-issued, and
        # the result report owns its own retry policy above.
        http.max_retries = 0

        http.start { |connection| connection.request(request) }
      rescue *CONNECT_ERRORS => e
        raise RetryableConnectionError if retry_connect

        raise Unavailable, "The Laravel Cloud agent runtime socket is unreachable (#{e.class}: #{e.message})."
      rescue *TRANSPORT_ERRORS => e
        raise Unavailable, "The Laravel Cloud agent request failed (#{e.class}: #{e.message})."
      end

      # Net::HTTP speaking HTTP/1.1 over a unix socket instead of TCP — the
      # agent's only transport. Everything above the socket (framing, keep-alive
      # handling, timeouts) is stock Net::HTTP.
      class UnixHTTP < Net::HTTP
        attr_accessor :socket_path

        private

        def connect
          socket = begin
            UNIXSocket.new(socket_path)
          rescue ArgumentError => e
            # A path the OS can't use at all (e.g. over the ~104-byte limit) —
            # misconfiguration, not a hiccup worth retrying.
            raise Unavailable, "The Laravel Cloud agent socket path #{socket_path.inspect} is unusable: #{e.message}"
          end

          @socket = Net::BufferedIO.new(
            socket,
            read_timeout: @read_timeout,
            write_timeout: @write_timeout,
            continue_timeout: @continue_timeout,
            debug_output: @debug_output,
          )

          on_connect
        end
      end
      private_constant :UnixHTTP
    end
  end
end
