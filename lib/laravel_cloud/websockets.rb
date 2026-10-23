require "action_cable"
require "net/http"
require "openssl"
require "digest"
require "securerandom"

module LaravelCloud
  module Websockets
    class UnsupportedFeature < StandardError; end

    def self.enabled?
      %w[PUSHER_APP_ID PUSHER_APP_KEY PUSHER_APP_SECRET PUSHER_HOST PUSHER_PORT PUSHER_SCHEME].all? do |key|
        ENV[key] && !ENV[key].empty?
      end
    end

    def self.channel(stream)
      "private-cloud-#{Digest::SHA256.hexdigest(stream)}"
    end

    def self.authorize(socket_id, channel)
      signature = OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), "#{socket_id}:#{channel}")
      "#{ENV.fetch('PUSHER_APP_KEY')}:#{signature}"
    end

    def self.broadcast(stream, payload)
      body = JSON.generate(name: "cloud-message", channels: [channel(stream)], data: payload)
      path = "/apps/#{ENV.fetch('PUSHER_APP_ID')}/events"
      parameters = {
        "auth_key" => ENV.fetch("PUSHER_APP_KEY"),
        "auth_timestamp" => Time.now.to_i.to_s,
        "auth_version" => "1.0",
        "body_md5" => Digest::MD5.hexdigest(body)
      }
      query = URI.encode_www_form(parameters.sort)
      signature = OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), "POST\n#{path}\n#{query}")
      uri = URI::HTTP.build(
        host: ENV.fetch("PUSHER_HOST"),
        port: Integer(ENV.fetch("PUSHER_PORT")), path: path,
        query: "#{query}&auth_signature=#{signature}"
      )
      uri.scheme = ENV.fetch("PUSHER_SCHEME")
      request = Net::HTTP::Post.new(uri.request_uri, "Content-Type" => "application/json")
      request.body = body
      response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https", open_timeout: 5, read_timeout: 5) do |http|
        http.request(request)
      end
      raise "Pusher broadcast failed: HTTP #{response.code}" unless response.is_a?(Net::HTTPSuccess)
    end

    module Streams
      def stream_from(broadcasting, callback = nil, coder: nil, &block)
        return super unless connection.respond_to?(:cloud_bridge?)
        if callback || coder || block
          raise UnsupportedFeature, "Custom stream callbacks and coders are not supported by this prototype"
        end
        names = connection.cloud_streams[identifier] ||= []
        names << broadcasting.to_s unless names.include?(broadcasting.to_s)
      end

      def stop_stream_from(broadcasting)
        return super unless connection.respond_to?(:cloud_bridge?)
        connection.cloud_streams.fetch(identifier, []).delete(broadcasting.to_s)
      end

      def stop_all_streams
        return super unless connection.respond_to?(:cloud_bridge?)
        connection.cloud_streams.delete(identifier)
      end
    end

    module ConnectionTransport
      attr_reader :cloud_frames, :cloud_streams

      def prepare_cloud_bridge
        @cloud_frames = []
        @cloud_streams = {}
      end

      def cloud_bridge?
        true
      end

      def transmit(frame)
        @cloud_frames << frame
      end

      def close(reason: nil, reconnect: true)
        transmit(type: "disconnect", reason: reason, reconnect: reconnect)
      end
    end

    class Sessions
      Session = Struct.new(:connection, :owner, :socket_id, :last_seen, :mutex, keyword_init: true)

      def initialize
        @sessions = {}
        @mutex = Mutex.new
      end

      def execute(payload, request)
        expire
        owner = request.session[:laravel_cloud_socket_owner] ||= SecureRandom.hex(24)
        if payload.fetch("command") == "connect"
          return connect(payload, request, owner)
        end

        token = payload.fetch("token")
        session = @mutex.synchronize { @sessions[token] }
        return [404, { error: "Connection expired" }] unless session && session.owner == owner

        session.mutex.synchronize do
          session.last_seen = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          connection = session.connection
          connection.cloud_frames.clear
          case payload.fetch("command")
          when "disconnect"
            connection.subscriptions.unsubscribe_from_all
            connection.disconnect if connection.respond_to?(:disconnect)
            @mutex.synchronize { @sessions.delete(token) }
          when "ping"
            connection.transmit(type: "ping", message: Time.now.to_i)
          else
            connection.handle_channel_command(payload.except("token"))
          end
          [200, snapshot(session).merge(token: token)]
        end
      end

      private

      def connect(payload, request, owner)
        socket_id = payload.fetch("socket_id")
        raise ArgumentError, "Invalid socket ID" unless socket_id.match?(/\A\d+\.\d+\z/)
        url = URI.parse(payload.fetch("url", "/cable"))
        environment = request.env.merge("PATH_INFO" => url.path, "QUERY_STRING" => url.query.to_s)
        connection = ActionCable.server.config.connection_class.call.new(ActionCable.server, environment)
        connection.extend(ConnectionTransport)
        connection.prepare_cloud_bridge
        connection.connect if connection.respond_to?(:connect)
        connection.transmit(type: "welcome")
        token = SecureRandom.hex(32)
        session = Session.new(
          connection: connection, owner: owner, socket_id: socket_id,
          last_seen: Process.clock_gettime(Process::CLOCK_MONOTONIC), mutex: Mutex.new
        )
        @mutex.synchronize { @sessions[token] = session }
        [200, snapshot(session).merge(token: token)]
      end

      def snapshot(session)
        subscriptions = session.connection.cloud_streams.transform_values do |streams|
          streams.map do |stream|
            name = Websockets.channel(stream)
            { name: name, auth: Websockets.authorize(session.socket_id, name) }
          end
        end
        { frames: session.connection.cloud_frames.dup, subscriptions: subscriptions }
      end

      def expire
        cutoff = Process.clock_gettime(Process::CLOCK_MONOTONIC) - 120
        expired = @mutex.synchronize do
          tokens = @sessions.select { |_, session| session.last_seen < cutoff }.keys
          tokens.map { |token| @sessions.delete(token) }
        end
        expired.each do |session|
          session.mutex.synchronize do
            session.connection.subscriptions.unsubscribe_from_all
            session.connection.disconnect if session.connection.respond_to?(:disconnect)
          end
        end
      end
    end

    class Middleware
      def initialize(app)
        @app = app
        @sessions = Sessions.new
      end

      def call(environment)
        request = ActionDispatch::Request.new(environment)
        if request.path == "/__laravel_cloud/cable.js" && request.get?
          return [200, { "content-type" => "text/javascript", "cache-control" => "no-store" }, [script]]
        end
        if request.path == "/__laravel_cloud/cable" && request.post?
          return command(request)
        end

        status, headers, body = @app.call(environment)
        return [status, headers, body] unless headers["content-type"].to_s.include?("text/html")

        html = +""
        body.each { |part| html << part }
        body.close if body.respond_to?(:close)
        html.sub!(/<head(?:\s[^>]*)?>/i) { |head| "#{head}<script src=\"/__laravel_cloud/cable.js\"></script>" }
        [status, headers.except("content-length", "etag"), [html]]
      end

      private

      def command(request)
        return json(403, error: "Invalid origin") unless request.get_header("HTTP_ORIGIN") == request.base_url
        return json(415, error: "JSON required") unless request.media_type == "application/json"
        payload = JSON.parse(request.raw_post)
        status, response = @sessions.execute(payload, request)
        json(status, response)
      rescue ActionCable::Connection::Authorization::UnauthorizedError
        json(401, error: "Unauthorized connection")
      rescue JSON::ParserError, KeyError, ArgumentError => error
        json(400, error: error.message)
      end

      def json(status, payload)
        [status, { "content-type" => "application/json", "cache-control" => "no-store" }, [JSON.generate(payload)]]
      end

      def script
        config = {
          key: ENV.fetch("PUSHER_APP_KEY"), host: ENV.fetch("PUSHER_HOST"),
          port: Integer(ENV.fetch("PUSHER_PORT")), scheme: ENV.fetch("PUSHER_SCHEME")
        }
        "globalThis.__laravelCloudWebsockets = #{JSON.generate(config)};\n" + File.read(File.join(__dir__, "websockets", "transport.js"))
      end
    end

    class Railtie < Rails::Railtie
      initializer "laravel_cloud.websockets" do |app|
        app.middleware.use(Middleware) if Websockets.enabled?
      end

      config.after_initialize do
        if Websockets.enabled?
          ActionCable.server.config.cable = { "adapter" => "laravel_cloud" }
          ActionCable::Channel::Base.prepend(Streams)
        end
      end
    end
  end
end
