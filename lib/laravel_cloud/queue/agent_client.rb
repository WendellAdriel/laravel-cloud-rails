# frozen_string_literal: true

require "json"
require "net/http"
require "socket"

module LaravelCloud
  module Queue
    # Talks to the Cloud agent over its unix socket. The agent long-polls SQS,
    # hands messages over on GET /next, and on POST /result performs the
    # delete or visibility change itself.
    class AgentClient
      # The agent is unreachable or erroring. It only recovers on restart, so
      # the worker exits; SQS redelivers anything in flight.
      class Unavailable < LaravelCloud::Error; end

      # The agent refused one outcome (HTTP 4xx); the worker logs it and moves on.
      class Rejected < LaravelCloud::Error; end

      # Longer than the agent's ~55s poll, so a poll never times out early.
      NEXT_TIMEOUT = 65
      RESULT_TIMEOUT = 10
      RESULT_ATTEMPTS = 3

      # The agent hasn't seen the request yet, so these are safe to retry.
      CONNECT_ERRORS = [Errno::ENOENT, Errno::ECONNREFUSED].freeze

      # The agent may already have acted, so these aren't retried.
      TRANSPORT_ERRORS = [
        Errno::ECONNRESET, Errno::EPIPE, EOFError, IOError,
        Net::ReadTimeout, Net::WriteTimeout, Net::OpenTimeout, Net::HTTPBadResponse,
      ].freeze

      def initialize(socket_path)
        @socket_path = socket_path
      end

      # The next message (messageId, receiptHandle, body, attributes, queueUrl),
      # or nil when there's none.
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

      # "processed" deletes the message; "released" redelivers it after `delay`
      # seconds. There's no "failed" outcome — failures are reported as
      # "processed" and surfaced through observability.
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
        # Never re-issue a long-poll; #report retries on its own terms.
        http.max_retries = 0

        http.start { |connection| connection.request(request) }
      rescue *CONNECT_ERRORS => e
        raise RetryableConnectionError if retry_connect

        raise Unavailable, "The Laravel Cloud agent runtime socket is unreachable (#{e.class}: #{e.message})."
      rescue *TRANSPORT_ERRORS => e
        raise Unavailable, "The Laravel Cloud agent request failed (#{e.class}: #{e.message})."
      end

      # Net::HTTP over a unix socket instead of TCP.
      class UnixHTTP < Net::HTTP
        attr_accessor :socket_path

        private

        def connect
          socket = begin
            UNIXSocket.new(socket_path)
          rescue ArgumentError => e
            # e.g. a path over the OS's ~104-byte limit: not worth retrying.
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
