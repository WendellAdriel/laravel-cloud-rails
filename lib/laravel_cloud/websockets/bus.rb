module LaravelCloud
  module Websockets
    class Bus
      def initialize(serial: false, &on_failure)
        @failure = on_failure
        @monitor = Monitor.new
        @changed = @monitor.new_cond
        @subscriptions = {}
        @joined = {}
        @closed = false
        @last_received = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @executor = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: serial ? 1 : 8, max_queue: 1024, fallback_policy: :abort)
        @thread = Thread.new { run }
        @thread.name = "rails-on-cloud-reverb"
      end

      def url
        config = Websockets.settings
        scheme = config[:scheme] == "https" ? "wss" : "ws"
        "#{scheme}://#{config[:host]}:#{config[:port]}/app/#{URI.encode_www_form_component(config[:key])}?protocol=7&client=rails-on-cloud&version=1"
      end

      def write(bytes)
        @socket.write(bytes)
      end

      def subscribe(channel, callback, success = nil, channel_data: nil)
        @monitor.synchronize do
          wait_until { @socket_id }
          if !@subscriptions.key?(channel)
            @subscriptions[channel] = []
            data = { channel: channel, auth: Websockets.authorize(@socket_id, channel, channel_data) }
            data[:channel_data] = channel_data if channel_data
            send_event(event: "pusher:subscribe", data: data)
          end
          @subscriptions[channel] << callback
          wait_until { @joined[channel] }
        end
        success.call if success
        @monitor.synchronize { @joined[channel] }
      end

      def unsubscribe(channel, callback)
        @monitor.synchronize do
          callbacks = @subscriptions[channel]
          return unless callbacks
          callbacks.delete(callback)
          if callbacks.empty?
            @subscriptions.delete(channel)
            @joined.delete(channel)
            send_event(event: "pusher:unsubscribe", data: { channel: channel }) unless @closed
          end
        end
      end

      def ping
        @monitor.synchronize do
          raise Unavailable, "Backend heartbeat timed out" if @closed || Process.clock_gettime(Process::CLOCK_MONOTONIC) - @last_received > 12
          send_event(event: "pusher:ping", data: {})
        end
      end

      def close
        @monitor.synchronize do
          return if @closed
          @closed = true
          @changed.broadcast
          @socket&.close
        end
        @executor.shutdown
      end

      private

      def wait_until
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
        until yield
          raise Unavailable, "WebSocket backend connection closed" if @closed
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise Unavailable, "WebSocket backend subscription timed out" if remaining <= 0
          @changed.wait(remaining)
        end
      end

      def application_origin
        uri = URI.parse(ENV["APP_URL"].to_s)
        return unless uri.is_a?(URI::HTTP) && uri.host
        port = uri.port unless uri.port == uri.default_port
        URI::Generic.build(scheme: uri.scheme, host: uri.host, port: port).to_s
      end

      def run
        config = Websockets.settings
        tcp = Socket.tcp(config[:host], config[:port], connect_timeout: 3)
        @socket = tcp
        if config[:scheme] == "https"
          context = OpenSSL::SSL::SSLContext.new
          context.set_params
          tls = OpenSSL::SSL::SSLSocket.new(tcp, context)
          tls.hostname = config[:host]
          tls.sync_close = true
          @socket = tls
          Timeout.timeout(3, Unavailable, "Backend TLS handshake timed out") do
            tls.connect
            tls.post_connection_check(config[:host])
          end
        end
        @driver = WebSocket::Driver.client(self, max_length: 1_000_000)
        origin = application_origin
        @driver.set_header("Origin", origin) if origin
        @driver.on(:message) { |event| receive(JSON.parse(event.data)) }
        @driver.on(:error) { |event| raise Unavailable, event.message }
        @driver.on(:close) { raise Unavailable, "Backend WebSocket closed" }
        @monitor.synchronize { @driver.start }
        loop do
          data = @socket.readpartial(16384)
          @monitor.synchronize { @driver.parse(data) }
        end
      rescue StandardError => error
        ActionCable.server.logger.warn("rails-on-cloud: backend connection lost (#{error.class})") unless @closed
      ensure
        close
        @failure.call
      end

      def send_event(event)
        @driver.text(JSON.generate(event))
      end

      def receive(frame)
        @last_received = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        data = frame["data"].is_a?(String) ? JSON.parse(frame["data"]) : frame["data"]
        case frame["event"]
        when "pusher:connection_established"
          @socket_id = data.fetch("socket_id")
          @changed.broadcast
        when "pusher_internal:subscription_succeeded"
          @joined[frame.fetch("channel")] = data
          @changed.broadcast
        when "pusher:ping"
          send_event(event: "pusher:pong", data: {})
        when "pusher:error", "pusher:subscription_error"
          raise Unavailable, "Backend subscription failed"
        else
          @subscriptions.fetch(frame["channel"], []).dup.each do |callback|
            @executor.post { callback.call(frame["event"], data) }
          end
        end
      end
    end
  end
end
