# frozen_string_literal: true

require "json"
require "socket"

module LaravelCloud
  # Writes Laravel Cloud observability events to the in-container log socket.
  #
  # This is a port of Laravel's Illuminate\Foundation\Cloud\Events: it speaks
  # the same wire protocol (newline-delimited JSON over a persistent stream
  # socket) so a Rails app surfaces the exact same metrics in the Laravel Cloud
  # dashboard as a Laravel app would.
  #
  # Every failure path is swallowed: observability must never take down a job
  # or a request.
  class Events
    # How long a connect or write may block before the event is dropped.
    TIMEOUT = 2

    # The address is a stream-socket address such as
    # "unix:///tmp/cloud-init.sock" (a bare path is accepted too).
    def initialize(address)
      @path = address.to_s.delete_prefix("unix://")
      @socket = nil

      # Enqueues emit from every request thread in a threaded server, so the
      # shared socket is written to under a lock to keep lines from interleaving.
      @mutex = Mutex.new
    end

    def emit(payload)
      emit_many([payload])
    end

    def emit_many(payloads)
      return if payloads.empty?

      line = payloads.map { |payload| JSON.generate(payload) }.join("\n") << "\n"

      @mutex.synchronize do
        ensure_connected
        write(line)
      end
    rescue StandardError
      # Observability is best-effort; a lost event is preferable to a failure.
    end

    private

    def write(line)
      @socket.write(line)
    rescue StandardError
      # A failed write leaves the stream in an unknown state, so drop it and let
      # the next emit reconnect rather than writing into a half-sent line.
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

    # Whether the socket is still usable. A peer that hung up reads as EOF; a
    # healthy idle socket has nothing to read, so a non-blocking peek tells the
    # two apart without consuming anything meaningful (the collector never
    # writes back).
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
