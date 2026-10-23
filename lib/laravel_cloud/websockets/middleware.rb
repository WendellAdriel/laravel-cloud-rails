module LaravelCloud
  module Websockets
    class Middleware
      ENDPOINT = "/__laravel_cloud/cable"
      COMMANDS = %w[connect ready lease disconnect subscribe unsubscribe message].freeze

      def initialize(app)
        @app = app
      end

      def call(environment)
        request = ActionDispatch::Request.new(environment)
        if request.path == "#{ENDPOINT}.js" && request.get?
          return with_owner(request, [200, { "content-type" => "text/javascript", "cache-control" => "no-store", "x-content-type-options" => "nosniff" }, [script]])
        end
        return command(request) if request.path == ENDPOINT && request.post?

        status, headers, body = @app.call(environment)
        return [status, headers, body] unless status == 200 && headers["content-type"].to_s.include?("text/html") && !headers["content-encoding"]
        nonce = request.content_security_policy_nonce
        tag = "<script src=\"#{ENDPOINT}.js\"#{nonce ? " nonce=\"#{ERB::Util.html_escape(nonce)}\"" : ''}></script>"
        with_owner(request, [status, headers.except("content-length", "etag"), HtmlBody.new(body, tag)])
      end

      private

      def command(request)
        return json(403, error: "Invalid origin") unless request.get_header("HTTP_ORIGIN") == request.base_url
        return json(415, error: "JSON required") unless request.media_type == "application/json"
        return json(413, error: "Command too large") if request.content_length.to_i > MAX_BYTES
        raw = request.body.read(MAX_BYTES + 1)
        return json(413, error: "Command too large") if raw.bytesize > MAX_BYTES
        payload = JSON.parse(raw)
        raise InvalidCommand, "Invalid command" unless payload.is_a?(Hash) && COMMANDS.include?(payload["command"])
        unless payload["command"] == "connect"
          raise InvalidCommand, "Request ID required" unless payload["request_id"].is_a?(String) && payload["request_id"].bytesize.between?(1, 64)
        end
        if %w[subscribe unsubscribe message].include?(payload["command"])
          raise InvalidCommand, "Invalid identifier" unless payload["identifier"].is_a?(String) && JSON.parse(payload["identifier"]).is_a?(Hash)
          raise InvalidCommand, "Invalid message" if payload["command"] == "message" && (!payload["data"].is_a?(String) || !JSON.parse(payload["data"]).is_a?(Hash))
        end
        return json(401, error: "Connection cookie required") unless Websockets.owner(request)
        status, response = Websockets.runtime.execute(payload, request)
        json(status, response)
      rescue JSON::ParserError, KeyError, ArgumentError, URI::InvalidURIError, InvalidCommand
        json(400, error: "Invalid channel command")
      rescue Unavailable
        json(503, error: "WebSocket backend unavailable")
      end

      def with_owner(request, response)
        unless Websockets.owner(request)
          Rack::Utils.set_cookie_header!(response[1], "__laravel_cloud_owner", value: Websockets.owner_cookie, path: "/", httponly: true, same_site: :lax, secure: request.ssl?)
        end
        response
      end

      def json(status, payload)
        [status, { "content-type" => "application/json", "cache-control" => "no-store" }, [JSON.generate(payload)]]
      end

      def script
        "globalThis.__laravelCloudWebsockets = #{JSON.generate(Websockets.settings)};\n" + File.read(File.join(__dir__, "transport.js"))
      end
    end

    class HtmlBody
      def initialize(body, tag)
        @body, @tag = body, tag
      end

      def each
        buffer = +""
        pending = true
        @body.each do |part|
          unless pending
            yield part
            next
          end
          buffer << part
          if buffer.sub!(/<head(?:\s[^>]*)?>/i) { |head| "#{head}#{@tag}" } || buffer.bytesize > 65536
            yield buffer
            buffer = +""
            pending = false
          end
        end
        yield buffer unless buffer.empty?
      end

      def close
        @body.close if @body.respond_to?(:close)
      end
    end
  end
end
