module LaravelCloud
  module Websockets
    module ConnectionTransport
      attr_reader :cloud_session

      def transmit(frame)
        @cloud_session.deliver(frame)
      end

      def close(reason: nil, reconnect: true)
        @cloud_session.close(reason: reason, reconnect: reconnect)
      end
    end

    class Session
      attr_reader :connection, :owner, :socket_id, :token, :last_seen, :protocol

      def initialize(runtime, token, owner, socket_id, environment, protocol = "actioncable-v1-json")
        @runtime, @token, @owner, @socket_id = runtime, token, owner, socket_id
        @protocol = protocol
        @presence = {}
        @history = {}
        @presence_joins = {}
        @seen = {}
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

      def channel
        "private-roc-client-#{Digest::SHA256.hexdigest(@token)}"
      end

      def deliver(frame)
        @lock.synchronize do
          return if @closed
          position = Thread.current[:laravel_cloud_stream_position]
          if @protocol == "actioncable-v1-ext-json" && !frame[:type] && frame.key?(:message) && frame[:identifier] && position
            frame = frame.merge(position.transform_keys(&:to_sym))
          end
          if @active
            @sequence += 1
            begin
              Websockets.publish(channel, "cloud-frame", { sequence: @sequence, frame: frame })
            rescue InvalidCommand
              @sequence -= 1
              close(reason: "message_too_large", reconnect: false)
            end
          else
            raise InvalidCommand, "Too many initial connection frames" if @pending.size >= 256
            @pending << frame
          end
          if frame[:type] == "confirm_subscription"
            if (presence = @presence_joins.delete(frame[:identifier]))
              join_presence(frame[:identifier], nil, presence.fetch("id"), presence["info"])
            end
            if (history = @history.delete(frame[:identifier]))
              replay(frame[:identifier], history)
            end
          elsif frame[:type] == "reject_subscription"
            @history.delete(frame[:identifier])
            @presence_joins.delete(frame[:identifier])
          end
        end
      end

      def dispatch_stream(identifier, message)
        @lock.synchronize do
          return if @closed
          position = message.respond_to?(:position) && message.position
          if @protocol == "actioncable-v1-ext-json" && position
            fingerprint = [identifier, position.fetch("stream_id"), position.fetch("epoch"), position.fetch("offset")]
            return if @seen.key?(fingerprint)
          end
          Websockets.with_position(message) { yield }
          if fingerprint
            @seen[fingerprint] = true
            @seen.shift if @seen.size > 1000
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
          when "lease", "pong"
          when "history"
            replay(payload.fetch("identifier"), payload.fetch("history"))
          when "join", "leave", "presence"
            raise InvalidCommand, "Extended protocol required" unless @protocol == "actioncable-v1-ext-json"
            presence = presence_for(payload.fetch("identifier"))
            case payload.fetch("command")
            when "join"
              value = payload.fetch("presence")
              presence.join(value.fetch("id"), value["info"])
            when "leave"
              presence.leave
            when "presence"
              deliver(type: "presence", identifier: payload.fetch("identifier"), message: presence.info(return_records: payload.dig("data", "return_records") != false))
            end
          when "unsubscribe"
            @history.delete(payload.fetch("identifier"))
            @presence_joins.delete(payload.fetch("identifier"))
            @presence.delete(payload.fetch("identifier"))&.close
            @connection.handle_channel_command(payload.except("token", "request_id"))
          when "subscribe"
            @presence_joins[payload.fetch("identifier")] = payload["presence"] if payload["presence"] && @protocol == "actioncable-v1-ext-json"
            @history[payload.fetch("identifier")] = payload["history"] if payload["history"] && @protocol == "actioncable-v1-ext-json"
            @connection.handle_channel_command(payload.except("token", "request_id"))
          when "whisper"
            subscription = subscription(payload)
            unless subscription.respond_to?(:whisper_stream) && subscription.whisper_stream
              return [403, { error: "Whispering is not authorized" }]
            end
            @connection.handle_channel_command(payload.except("token", "request_id"))
          else
            @connection.handle_channel_command(payload.except("token", "request_id"))
          end
          result = [200, { ok: true }]
          @requests[request_id] = result
          @requests.shift if @requests.size > 128
          result
        end
      end

      def join_presence(identifier, stream, id, info)
        @lock.synchronize do
          return if @closed
          raise InvalidCommand, "Invalid presence ID" unless id.bytesize.between?(1, 200)
          presence_for(identifier, stream).join(id, info)
        end
      end

      def leave_presence(identifier, id)
        @lock.synchronize { @presence[identifier]&.leave(id) unless @closed }
      end

      def presence_for(identifier, stream = nil)
        channel = subscription("identifier" => identifier)
        current = @presence[identifier]
        return current if current && (!stream || current.stream == stream)
        current&.close
        stream ||= channel.send(:streams).keys.first
        raise InvalidCommand, "A presence stream is required" unless stream
        @presence[identifier] = Presence.new(self, identifier, stream)
      end

      def ping
        @lock.synchronize { @presence.each_value(&:ping) }
      rescue Unavailable
        close(reason: "presence_backend_unavailable")
      end

      def replay(identifier, request)
        channel = subscription("identifier" => identifier)
        broker = Websockets.broker
        unless broker
          deliver(type: "reject_history", identifier: identifier)
          return
        end
        batches = channel.send(:streams).keys.map do |stream|
          prefixed = ActionCable.server.pubsub.send(:channel_with_prefix, stream)
          position = request.fetch("streams", {}).fetch(broker.stream_id(prefixed), {})
          [stream, broker.read(prefixed, position, since: request["since"])]
        end
        if batches.any? { |_, entries| entries.nil? }
          deliver(type: "reject_history", identifier: identifier)
          return
        end
        batches.each { |stream, entries| entries.each { |entry| channel.send(:cloud_replay, stream, entry) } }
        deliver(type: "confirm_history", identifier: identifier)
      end

      def subscription(payload)
        raise InvalidCommand, "Unknown subscription" unless @connection.subscriptions.identifiers.include?(payload["identifier"])
        @connection.subscriptions.send(:find, payload)
      end

      def close(reason: nil, reconnect: true)
        @lock.synchronize do
          return if @closed
          begin
            deliver(type: "disconnect", reason: reason, reconnect: reconnect) unless reason == "client_disconnect"
          rescue Unavailable
          ensure
            @closed = true
            @presence.each_value(&:close)
            @presence.clear
            begin
              @connection.send(:handle_close)
            ensure
              @runtime.remove(@token)
            end
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
        broker = Websockets.broker
        cursor = broker&.snapshot(stream)&.slice("epoch", "offset")
        cursor = {} if cursor && cursor["epoch"].empty?
        lock = Monitor.new
        drain = lambda do
          lock.synchronize do
            entries = broker.read(stream, cursor)
            raise Unavailable, "Live stream history expired" unless entries
            entries.each do |entry|
              callback.call(StreamMessage.new(entry))
              cursor = entry.slice("epoch", "offset")
            end
          end
        end
        wrapped = lambda do |event, data|
          if event == "cloud-stream" && broker
            lock.synchronize do
              same_epoch = cursor["epoch"] == data.fetch("epoch")
              if same_epoch && data.fetch("offset") == cursor.fetch("offset", 0) + 1
                callback.call(StreamMessage.new(data))
                cursor = data.slice("epoch", "offset")
              elsif !same_epoch || data.fetch("offset") > cursor.fetch("offset", 0)
                drain.call
              end
            end
          elsif event == "cloud-stream"
            callback.call(StreamMessage.new(data))
          elsif event == "cloud-broadcast"
            callback.call(data)
          end
        rescue Unavailable
          stop
        end
        @lock.synchronize { @listeners[[stream, callback]] = wrapped }
        @bus.subscribe(Websockets.channel(stream), wrapped, success)
        drain.call if broker
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
        ActionCable.server.connections.dup.each do |connection|
          connection.close(reason: "server_restart") unless connection.is_a?(ConnectionTransport)
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
        protocol = payload.fetch("protocol", "actioncable-v1-json")
        raise InvalidCommand, "Unsupported Cable protocol" unless %w[actioncable-v1-json actioncable-v1-ext-json].include?(protocol)
        session = Session.new(self, token, owner, socket_id, environment, protocol)
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
      rescue InvalidCommand, KeyError
        [400, { error: "Invalid channel command" }]
      rescue Unavailable
        [503, { error: "WebSocket backend unavailable" }]
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
        @bus.ping
        sessions = @lock.synchronize { @sessions.values.dup }
        return if sessions.empty?
        cutoff = Process.clock_gettime(Process::CLOCK_MONOTONIC) - 90
        sessions.each do |session|
          session.ping
          Rails.application.executor.wrap { session.close(reason: "connection_timeout") } if session.last_seen < cutoff
        end
        Websockets.publish(heartbeat_channel, "cloud-heartbeat", { time: Time.now.to_i })
      rescue Unavailable
        stop
      end
    end
  end
end
