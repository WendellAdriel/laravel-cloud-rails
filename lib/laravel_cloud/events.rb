# frozen_string_literal: true

require "json"
require "socket"

module LaravelCloud
  # Writes dashboard events to the Cloud log socket as newline-delimited JSON.
  # A port of Laravel's Cloud\Events. Failures are swallowed: observability
  # must never take down a job or a request.
  class Events
    TIMEOUT = 2

    # e.g. "unix:///tmp/cloud-init.sock", or a bare path.
    def initialize(address)
      @path = address.to_s.delete_prefix("unix://")
      @socket = nil

      # Request threads all enqueue, so lock to keep lines from interleaving.
      @mutex = Mutex.new
    end

    def emit(payload)
      emit_many([payload])
    end

    def emit_many(payloads)
      return if payloads.empty?

      line = payloads.map { |payload| JSON.generate(scrub(payload)) }.join("\n") << "\n"

      @mutex.synchronize do
        ensure_connected
        write(line)
      end
    rescue StandardError
    end

    private

    # Like Laravel's JSON_INVALID_UTF8_SUBSTITUTE: bad bytes become U+FFFD.
    # Ruby's JSON would raise instead, silently dropping the event.
    def scrub(value)
      case value
      when String then value.valid_encoding? && value.encoding == Encoding::UTF_8 ? value : value.dup.force_encoding(Encoding::UTF_8).scrub("\uFFFD")
      when Hash then value.to_h { |key, item| [scrub(key), scrub(item)] }
      when Array then value.map { |item| scrub(item) }
      else value
      end
    end

    def write(line)
      @socket.write(line)
    rescue StandardError
      # The stream may hold a half-written line; reconnect on the next emit.
      disconnect
      raise
    end

    def ensure_connected
      connect unless connected?
    end

    def connect
      socket = Socket.unix(@path)
      socket.timeout = TIMEOUT
      @socket = socket
    end

    # A non-blocking read tells a closed peer (EOF) from a healthy idle socket
    # (nothing to read). The collector never writes back.
    def connected?
      return false if @socket.nil? || @socket.closed?

      if @socket.read_nonblock(1, exception: false).nil?
        disconnect
        return false
      end

      true
    rescue StandardError
      disconnect
      false
    end

    def disconnect
      @socket&.close
    rescue StandardError
      # Already gone.
    ensure
      @socket = nil
    end
  end
end
