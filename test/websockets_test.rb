require "test_helper"
require "rails"
require "rack/mock"
require_relative "../lib/laravel_cloud/websockets"

class WebsocketsTest < Minitest::Test
  VARIABLES = LaravelCloud::Websockets::VARIABLES

  def setup
    @environment = VARIABLES.to_h { |name| [name, ENV[name]] }
    ENV.update("PUSHER_APP_ID" => "example", "PUSHER_APP_KEY" => "public-key", "PUSHER_APP_SECRET" => "private-secret", "PUSHER_HOST" => "localhost", "PUSHER_PORT" => "8080", "PUSHER_SCHEME" => "http")
  end

  def teardown
    @environment.each { |key, value| value ? ENV[key] = value : ENV.delete(key) }
  end

  def test_activation_requires_all_six_variables
    assert LaravelCloud::Websockets.enabled?
    VARIABLES.each do |name|
      value = ENV.delete(name)
      refute LaravelCloud::Websockets.enabled?, name
      ENV[name] = value
    end
  end

  def test_private_channel_authorization_is_bound_to_socket_and_channel
    signature = LaravelCloud::Websockets.authorize("1.2", "private-room")
    expected = OpenSSL::HMAC.hexdigest("SHA256", "private-secret", "1.2:private-room")
    assert_equal "public-key:#{expected}", signature
    refute_equal signature, LaravelCloud::Websockets.authorize("1.3", "private-room")
    refute_equal signature, LaravelCloud::Websockets.authorize("1.2", "private-other")
  end

  def test_invalid_scheme_and_port_fail_before_connecting
    ENV["PUSHER_SCHEME"] = "file"
    assert_raises(LaravelCloud::Websockets::Error) { LaravelCloud::Websockets.settings }
    ENV["PUSHER_SCHEME"] = "https"
    ENV["PUSHER_PORT"] = "0"
    assert_raises(LaravelCloud::Websockets::Error) { LaravelCloud::Websockets.settings }
  end

  def test_command_validation_rejects_invalid_origin_type_size_and_shape
    app = LaravelCloud::Websockets::Middleware.new(->(_env) { [204, {}, []] })
    mock = Rack::MockRequest.new(app)
    headers = { "HTTP_ORIGIN" => "http://example.org", "CONTENT_TYPE" => "application/json" }
    assert_equal 403, mock.post("/__laravel_cloud/cable", **headers.merge("HTTP_ORIGIN" => "https://foreign.example"), input: "{}").status
    assert_equal 415, mock.post("/__laravel_cloud/cable", **headers.merge("CONTENT_TYPE" => "text/plain"), input: "{}").status
    assert_equal 413, mock.post("/__laravel_cloud/cable", **headers, input: "x" * 9000).status
    ["null", "[]", "{", '{"command":"unknown"}', '{"command":"message","request_id":"a","identifier":"[]","data":"{}"}'].each do |input|
      assert_equal 400, mock.post("/__laravel_cloud/cable", **headers, input: input).status
    end
  end

  def test_html_body_injects_across_chunk_boundaries_and_preserves_streaming
    chunks = ["<!doctype html><he", "ad>", "one", "two"]
    body = LaravelCloud::Websockets::HtmlBody.new(chunks, '<script src="/bridge.js"></script>')
    result = []
    body.each { |chunk| result << chunk }
    assert_equal ['<!doctype html><head><script src="/bridge.js"></script>', "one", "two"], result
  end

  def test_html_body_closes_underlying_response_and_bounds_head_search
    body = Class.new do
      attr_reader :closed
      def each
        yield "x" * 65537
        yield "<head>later</head>"
      end
      def close
        @closed = true
      end
    end.new
    wrapped = LaravelCloud::Websockets::HtmlBody.new(body, "injected")
    chunks = []
    wrapped.each { |chunk| chunks << chunk }
    wrapped.close
    assert body.closed
    assert_equal [65537, 18], chunks.map(&:bytesize)
    refute_includes chunks.join, "injected"
  end

  def test_bridge_cookie_is_signed_and_does_not_require_a_rails_session
    app = LaravelCloud::Websockets::Middleware.new(->(_env) { [204, {}, []] })
    response = Rack::MockRequest.new(app).get("/__laravel_cloud/cable.js")
    header = Array(response["set-cookie"]).join(";")
    assert_includes header.downcase, "httponly"
    cookie = header.split(";").first
    request = ActionDispatch::Request.new(Rack::MockRequest.env_for("/", "HTTP_COOKIE" => cookie))
    assert_match(/\A[0-9a-f]{48}\z/, LaravelCloud::Websockets.owner(request))
    altered = cookie.sub(/.$/) { |last| last == "0" ? "1" : "0" }
    request = ActionDispatch::Request.new(Rack::MockRequest.env_for("/", "HTTP_COOKIE" => altered))
    assert_nil LaravelCloud::Websockets.owner(request)
  end

  def test_script_contains_only_public_connection_settings
    app = LaravelCloud::Websockets::Middleware.new(->(_env) { [204, {}, []] })
    response = Rack::MockRequest.new(app).get("/__laravel_cloud/cable.js")
    assert_equal 200, response.status
    assert_includes response.body, "public-key"
    refute_includes response.body, "private-secret"
    assert_equal "no-store", response["cache-control"]
  end

  def test_publish_signs_the_actual_http_request
    listener = TCPServer.new("127.0.0.1", 0)
    ENV["PUSHER_PORT"] = listener.addr[1].to_s
    ENV["PUSHER_HOST"] = "127.0.0.1"
    captured = ::Queue.new
    thread = Thread.new do
      socket = listener.accept
      request_line = socket.gets
      headers = {}
      while (line = socket.gets) && line != "\r\n"
        key, value = line.split(":", 2)
        headers[key.downcase] = value.strip
      end
      body = socket.read(headers.fetch("content-length").to_i)
      captured << [request_line, body]
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
      socket.close
    end
    LaravelCloud::Websockets.publish("private-room", "cloud-frame", { text: "hello" })
    request_line, body = captured.pop(timeout: 2)
    uri = URI.parse(request_line.split[1])
    query = URI.decode_www_form(uri.query).to_h
    signature = query.delete("auth_signature")
    assert_equal Digest::MD5.hexdigest(body), query["body_md5"]
    expected = OpenSSL::HMAC.hexdigest("SHA256", "private-secret", "POST\n#{uri.path}\n#{URI.encode_www_form(query.sort)}")
    assert_equal expected, signature
    assert_equal({ "text" => "hello" }, JSON.parse(JSON.parse(body).fetch("data")))
  ensure
    listener&.close
    thread&.join(2)
  end
end
