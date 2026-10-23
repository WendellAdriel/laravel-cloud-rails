require "action_cable"
require "net/http"
require "openssl"
require "digest"
require "securerandom"
require "monitor"
require "socket"
require "websocket/driver"
require "erb"

module LaravelCloud
  module Websockets
    class Error < LaravelCloud::Error; end
    class Unavailable < Error; end
    class InvalidCommand < Error; end

    VARIABLES = %w[PUSHER_APP_ID PUSHER_APP_KEY PUSHER_APP_SECRET PUSHER_HOST PUSHER_PORT PUSHER_SCHEME].freeze
    MAX_BYTES = 8192

    def self.enabled?
      VARIABLES.all? { |key| ENV[key] && !ENV[key].empty? }
    end

    def self.settings
      scheme = ENV.fetch("PUSHER_SCHEME")
      raise Error, "PUSHER_SCHEME must be http or https" unless %w[http https].include?(scheme)
      port = Integer(ENV.fetch("PUSHER_PORT"))
      raise Error, "Invalid PUSHER_PORT" unless (1..65535).cover?(port)
      { host: ENV.fetch("PUSHER_HOST"), port: port, scheme: scheme, key: ENV.fetch("PUSHER_APP_KEY") }
    end

    def self.channel(stream)
      "private-roc-stream-#{Digest::SHA256.hexdigest(stream)}"
    end

    def self.authorize(socket_id, channel, channel_data = nil)
      value = [socket_id, channel, channel_data].compact.join(":")
      signature = OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), value)
      "#{ENV.fetch('PUSHER_APP_KEY')}:#{signature}"
    end

    def self.event_body(channel, event, data)
      body = JSON.generate(name: event, channels: [channel], data: JSON.generate(data))
      raise InvalidCommand, "Message exceeds the backend payload limit" if body.bytesize > 9500
      body
    end

    def self.publish(channel, event, data)
      body = event_body(channel, event, data)
      path = "/apps/#{ENV.fetch('PUSHER_APP_ID')}/events"
      parameters = { "auth_key" => ENV.fetch("PUSHER_APP_KEY"), "auth_timestamp" => Time.now.to_i.to_s, "auth_version" => "1.0", "body_md5" => Digest::MD5.hexdigest(body) }
      query = URI.encode_www_form(parameters.sort)
      signature = OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), "POST\n#{path}\n#{query}")
      config = settings
      klass = config[:scheme] == "https" ? URI::HTTPS : URI::HTTP
      uri = klass.build(host: config[:host], port: config[:port], path: path, query: "#{query}&auth_signature=#{signature}")
      response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https", open_timeout: 2, read_timeout: 3) do |http|
        http.post(uri.request_uri, body, { "Content-Type" => "application/json" })
      end
      raise Unavailable, "WebSocket backend returned HTTP #{response.code}" unless response.is_a?(Net::HTTPSuccess)
      true
    rescue IOError, SystemCallError, Timeout::Error, OpenSSL::SSL::SSLError => error
      raise Unavailable, "WebSocket backend unavailable (#{error.class})"
    end

    def self.owner(request)
      value = Rack::Utils.parse_cookies_header(request.get_header("HTTP_COOKIE").to_s)["__laravel_cloud_owner"]
      return unless value.is_a?(String) && value.ascii_only? && value.match?(/\A[0-9a-f]{48}\.[0-9a-f]{64}\z/)
      nonce, signature = value.split(".")
      expected = OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), "roc-owner:#{nonce}")
      nonce if ActiveSupport::SecurityUtils.secure_compare(expected, signature)
    end

    def self.owner_cookie
      nonce = SecureRandom.hex(24)
      signature = OpenSSL::HMAC.hexdigest("SHA256", ENV.fetch("PUSHER_APP_SECRET"), "roc-owner:#{nonce}")
      "#{nonce}.#{signature}"
    end

    def self.broker
      @broker_lock ||= Mutex.new
      @broker_lock.synchronize do
        if @broker_pid != Process.pid
          @broker = Broker.resolve
          @broker_pid = Process.pid
        end
        @broker
      end
    end

    def self.with_position(message)
      previous = Thread.current[:laravel_cloud_stream_position]
      Thread.current[:laravel_cloud_stream_position] = message.respond_to?(:position) ? message.position : nil
      yield
    ensure
      Thread.current[:laravel_cloud_stream_position] = previous
    end

    def self.runtime
      @runtime_lock ||= Mutex.new
      @runtime_lock.synchronize do
        if !@runtime || @runtime.pid != Process.pid || !@runtime.alive?
          @runtime = Runtime.new
        end
        @runtime
      end
    end

    module WhisperStreams
      def stream_from(broadcasting, callback = nil, **options, &block)
        self.whisper_stream = broadcasting.to_s if options[:whisper] && respond_to?(:whisper_stream=)
        super
      end
    end

    class Railtie < Rails::Railtie
      initializer "laravel_cloud.websockets", after: "action_cable.set_configs", before: "anycable.disable_action_cable_mount" do |app|
        if Websockets.enabled?
          Websockets.settings
          ActionCable.server.config.cable = (ActionCable.server.config.cable || {}).merge("adapter" => "laravel_cloud")
          app.middleware.use(Middleware)
        end
      end

      config.after_initialize do
        if Websockets.enabled?
          ActionCable::Channel::Base.prepend(WhisperStreams)
          ActionCable::Channel::Base.prepend(StreamPositions)
          if defined?(::AnyCable::Rails)
            require "anycable/rails/channel/presence"
            ::AnyCable::Rails::Channel::Presence.prepend(ChannelPresence)
          end
        end
      end
    end
  end
end

require_relative "websockets/bus"
require_relative "websockets/broker"
require_relative "websockets/presence"
require_relative "websockets/runtime"
require_relative "websockets/middleware"
