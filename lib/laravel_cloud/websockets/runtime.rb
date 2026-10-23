module LaravelCloud
  module Websockets
    module ConnectionTransport
      def transmit(frame)
        @cloud_session.deliver(frame)
      end

      def close(reason: nil, reconnect: true)
        @cloud_session.close(reason: reason, reconnect: reconnect)
      end
    end

    class Session
      attr_reader :connection, :owner, :socket_id, :token, :last_seen

      def initialize(runtime, token, owner, socket_id, environment)
        @runtime, @token, @owner, @socket_id = runtime, token, owner, socket_id
        @lock = Monitor.new
        @closed = @active = false
        @pending = []
        @sequence = 0
        @requests = {}
        @last_seen = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @connection = ActionCable.server.config.connection_class.call.new(ActionCable.server, environment)
        @connection.extend(ConnectionTransport)
        @connection.instance_variable_set(:@cloud_session, self)
        @connection.instance_variable_set(:@websocket, self)
      end

      def open
        @connection.send(:handle_open)
      end

      def alive?
        !@closed
      end

      def possible?
        true
      end

      def protocol
        "actioncable-v1-json"
      end

      def channel
        "private-roc-client-#{Digest::SHA256.hexdigest(@token)}"
      end

      def deliver(frame)
        @lock.synchronize do
          return if @closed
          if @active
            @sequence += 1
            Websockets.publish(channel, "cloud-frame", { sequence: @sequence, frame: frame })
          else
            raise InvalidCommand, "Too many initial connection frames" if @pending.size >= 256
            @pending << frame
          end
        end
      end

      def execute(payload)
        @lock.synchronize do
          return [410, { error: "Connection expired" }] if @closed
          @last_seen = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          request_id = payload.fetch("request_id")
          return @requests[request_id] if @requests.key?(request_id)
          case payload.fetch("command")
          when "ready"
            @active = true
            @pending.each { |frame| deliver(frame) }
            @pending.clear
          when "disconnect"
            close(reason: "client_disconnect", reconnect: false)
          when "lease"
          else
            @connection.handle_channel_command(payload.except("token", "request_id"))
          end
          result = [200, { ok: true }]
          @requests[request_id] = result
          @requests.shift if @requests.size > 128
          result
        end
      end

      def close(reason: nil, reconnect: true)
        @lock.synchronize do
          return if @closed
          begin
            deliver(type: "disconnect", reason: reason, reconnect: reconnect) unless reason == "client_disconnect"
          rescue Unavailable
          ensure
            @closed = true
            @connection.send(:handle_close)
            @runtime.remove(@token)
          end
        end
      end
    end

    class Runtime
      attr_reader :pid, :id

      def initialize
        @pid = Process.pid
        @id = SecureRandom.hex(16)
        @lock = Mutex.new
        @sessions, @pending, @listeners = {}, {}, {}
        @alive = true
        @bus = Bus.new { stop }
        @bus.subscribe(control_channel, method(:receive))
        @timer = ActionCable.server.event_loop.timer(3) { pulse }
      end

      def alive?
        @alive
      end

      def control_channel
        "private-roc-worker-#{@id}"
      end

      def heartbeat_channel
        "private-roc-live-#{@id}"
      end

      def subscribe(stream, callback, success = nil)
        wrapped = ->(event, data) { callback.call(data) if event == "cloud-broadcast" }
        @lock.synchronize { @listeners[[stream, callback]] = wrapped }
        @bus.subscribe(Websockets.channel(stream), wrapped, success)
      end

      def unsubscribe(stream, callback)
        wrapped = @lock.synchronize { @listeners.delete([stream, callback]) }
        @bus.unsubscribe(Websockets.channel(stream), wrapped) if wrapped
      end

      def execute(payload, request)
        owner = Websockets.owner(request)
        return connect(payload, request, owner) if payload.fetch("command") == "connect"
        worker = verify_token(payload.fetch("token"))
        return [404, { error: "Unknown connection" }] unless worker
        return local(payload, owner) if worker == @id

        request_id = SecureRandom.hex(16)
        queue = ::Queue.new
        @lock.synchronize { @pending[request_id] = queue }
        Websockets.publish("private-roc-worker-#{worker}", "cloud-rpc", { id: request_id, reply_to: control_channel, owner: owner, payload: payload })
        queue.pop(timeout: 5) || [410, { error: "Connection owner unavailable" }]
      ensure
        @lock.synchronize { @pending.delete(request_id) } if request_id
      end

      def remove(token)
        @lock.synchronize { @sessions.delete(token) }
      end

      def stop
        return unless @alive
        @alive = false
        @timer&.shutdown
        sessions = @lock.synchronize { @sessions.values.dup }
        sessions.each do |session|
          Rails.application.executor.wrap { session.close(reason: "server_restart") }
        rescue StandardError => error
          ActionCable.server.logger.error("rails-on-cloud cleanup failed (#{error.class})")
        end
        @bus.close
      end

      private

      def sign(value)
        OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), value)
      end

      def verify_token(token)
        return unless token.is_a?(String) && token.match?(/\A[0-9a-f]{32}\.[0-9a-f]{48}\.[0-9a-f]{64}\z/)
        worker, nonce, signature = token.split(".")
        worker if ActiveSupport::SecurityUtils.secure_compare(sign("#{worker}.#{nonce}"), signature)
      end

      def connect(payload, request, owner)
        socket_id = payload.fetch("socket_id")
        raise InvalidCommand, "Invalid socket ID" unless socket_id.is_a?(String) && socket_id.match?(/\A\d+\.\d+\z/)
        url = URI.parse(payload.fetch("url", "/cable"))
        raise InvalidCommand, "Invalid connection URL" unless url.path&.start_with?("/") && url.path.length < 2048
        value = "#{@id}.#{SecureRandom.hex(24)}"
        token = "#{value}.#{sign(value)}"
        environment = request.env.merge("PATH_INFO" => url.path, "QUERY_STRING" => url.query.to_s)
        session = Session.new(self, token, owner, socket_id, environment)
        @lock.synchronize do
          raise Unavailable, "Too many connections" if @sessions.size >= 10000 || @sessions.values.count { |s| s.owner == owner } >= 32
          @sessions[token] = session
        end
        session.connection.worker_pool.work(session.connection) { session.open }
        return [401, { error: "Unauthorized connection" }] unless session.alive?
        [200, { token: token, channels: [session.channel, heartbeat_channel].map { |channel| { name: channel, auth: Websockets.authorize(socket_id, channel) } } }]
      rescue StandardError
        session&.close(reason: "connection_error", reconnect: false)
        raise
      end

      def local(payload, owner)
        session = @lock.synchronize { @sessions[payload["token"]] }
        return [410, { error: "Connection expired" }] unless session
        return [404, { error: "Unknown connection" }] unless ActiveSupport::SecurityUtils.secure_compare(session.owner, owner)
        session.connection.worker_pool.work(session.connection) { session.execute(payload) }
      end

      def receive(event, data)
        if event == "cloud-reply"
          @lock.synchronize { @pending[data["id"]] }&.push(data.fetch("result"))
        elsif event == "cloud-rpc"
          result = Rails.application.executor.wrap { local(data.fetch("payload"), data.fetch("owner")) }
          Websockets.publish(data.fetch("reply_to"), "cloud-reply", { id: data.fetch("id"), result: result })
        end
      rescue StandardError => error
        ActionCable.server.logger.error("rails-on-cloud command failed (#{error.class})")
      end

      def pulse
        sessions = @lock.synchronize { @sessions.values.dup }
        return if sessions.empty?
        cutoff = Process.clock_gettime(Process::CLOCK_MONOTONIC) - 90
        sessions.each do |session|
          Rails.application.executor.wrap { session.close(reason: "connection_timeout") } if session.last_seen < cutoff
        end
        Websockets.publish(heartbeat_channel, "cloud-heartbeat", { time: Time.now.to_i })
      rescue Unavailable
        stop
      end
    end
  end
end
