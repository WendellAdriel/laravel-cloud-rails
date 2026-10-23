require "test_helper"
require "rails"
require "rack/mock"
require_relative "../lib/laravel_cloud/websockets"
require_relative "../lib/action_cable/subscription_adapter/laravel_cloud"

class WebsocketsTest < Minitest::Test
  VARIABLES = LaravelCloud::Websockets::VARIABLES

  def setup
    @environment = (VARIABLES + ["APP_URL"]).to_h { |name| [name, ENV[name]] }
    ENV.delete("APP_URL")
    ENV.update("PUSHER_APP_ID" => "example", "PUSHER_APP_KEY" => "public-key", "PUSHER_APP_SECRET" => "private-secret", "PUSHER_HOST" => "localhost", "PUSHER_PORT" => "8080", "PUSHER_SCHEME" => "http")
  end

  def teardown
    @environment.each { |key, value| value ? ENV[key] = value : ENV.delete(key) }
  end

  def test_existing_csp_adds_only_the_managed_socket_origin
    policy = ActionDispatch::ContentSecurityPolicy.new do |value|
      value.default_src :self
      value.script_src :self
      value.connect_src :self
    end
    original = policy.directives.deep_dup
    app = LaravelCloud::Websockets::Middleware.new(->(_env) { [200, { "content-type" => "text/html" }, ["<head></head><body>hello</body>"]] })
    mock = Rack::MockRequest.new(ActionDispatch::ContentSecurityPolicy::Middleware.new(app))
    response = mock.get("/", "action_dispatch.content_security_policy" => policy)
    assert_includes response["content-security-policy"], "connect-src 'self' ws://localhost:8080"
    assert_equal original, policy.directives
    assert_equal "private, no-store", response["cache-control"]
    refute_includes response["content-security-policy"], "unsafe-inline"
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

  def test_presence_authorization_covers_the_exact_membership_data
    data = JSON.generate(user_id: "member", user_info: { name: "Member" })
    signature = LaravelCloud::Websockets.authorize("1.2", "presence-room", data)
    expected = OpenSSL::HMAC.hexdigest("SHA256", "private-secret", "1.2:presence-room:#{data}")
    assert_equal "public-key:#{expected}", signature
    refute_equal signature, LaravelCloud::Websockets.authorize("1.2", "presence-room", data + " ")
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
    [
      { command: "join", request_id: "a", identifier: '{"channel":"RoomChannel"}', presence: { id: 1 } },
      { command: "history", request_id: "a", identifier: '{"channel":"RoomChannel"}', history: [] },
      { command: "history", request_id: "a", identifier: '{"channel":"RoomChannel"}', history: { streams: { stream: { offset: -1, epoch: "a" } } } }
    ].each do |payload|
      assert_equal 400, mock.post("/__laravel_cloud/cable", **headers, input: JSON.generate(payload)).status
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
    assert_equal "private, no-store", response["cache-control"]
  end

  def test_a_stalled_backend_tls_handshake_is_bounded
    listener = TCPServer.new("127.0.0.1", 0)
    ENV["PUSHER_PORT"] = listener.addr[1].to_s
    ENV["PUSHER_HOST"] = "127.0.0.1"
    ENV["PUSHER_SCHEME"] = "https"
    release = ::Queue.new
    failures = ::Queue.new
    old_logger = ActionCable.server.config.logger
    ActionCable.server.config.logger = Logger.new(File::NULL)
    peer = Thread.new do
      socket = listener.accept
      release.pop
      socket.close
    end
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    bus = LaravelCloud::Websockets::Bus.new { failures << true }
    assert_raises(LaravelCloud::Websockets::Unavailable) { bus.subscribe("private-test", ->(*) {}) }
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 4.5
    assert failures.pop(timeout: 1)
  ensure
    bus&.close
    release&.push(true)
    listener&.close
    peer&.join(2)
    ActionCable.server.config.logger = old_logger
  end

  def test_backend_handshake_sends_only_the_application_origin
    {
      "https://app.example/path" => "https://app.example",
      "https://ignored:ignored@app.example:8443/nested?ignored=true#fragment" => "https://app.example:8443"
    }.each do |url, origin|
      ENV["APP_URL"] = url
      assert_equal origin, capture_bus_handshake["origin"]
    end
  end

  def test_backend_handshake_does_not_require_an_application_url
    headers = capture_bus_handshake
    refute headers.key?("origin")
  end

  def test_publish_signs_the_actual_http_request
    request_line, body = capture_publish do
      LaravelCloud::Websockets.publish("private-room", "cloud-frame", { text: "hello" })
    end
    uri = URI.parse(request_line.split[1])
    query = URI.decode_www_form(uri.query).to_h
    signature = query.delete("auth_signature")
    assert_equal Digest::MD5.hexdigest(body), query["body_md5"]
    expected = OpenSSL::HMAC.hexdigest("SHA256", "private-secret", "POST\n#{uri.path}\n#{URI.encode_www_form(query.sort)}")
    assert_equal expected, signature
    assert_equal({ "text" => "hello" }, JSON.parse(JSON.parse(body).fetch("data")))
  end

  def test_subscription_adapter_preserves_the_application_channel_prefix
    config = ActionCable::Server::Configuration.new
    config.cable = { channel_prefix: "my-application" }
    server = ActionCable::Server::Base.new(config: config)
    adapter = ActionCable::SubscriptionAdapter::LaravelCloud.new(server)
    _, body = capture_publish { adapter.broadcast("room", JSON.generate(text: "hello")) }
    assert_equal [LaravelCloud::Websockets.channel("my-application:room")], JSON.parse(body).fetch("channels")
  end

  def test_backend_http_errors_are_reported_to_the_publisher
    capture_publish(status: 401) do
      error = assert_raises(LaravelCloud::Websockets::Unavailable) do
        LaravelCloud::Websockets.publish("private-room", "cloud-frame", { text: "hello" })
      end
      assert_match(/HTTP 401/, error.message)
    end
  end

  private

  def capture_bus_handshake
    listener = TCPServer.new("127.0.0.1", 0)
    ENV["PUSHER_PORT"] = listener.addr[1].to_s
    ENV["PUSHER_HOST"] = "127.0.0.1"
    captured = ::Queue.new
    old_logger = ActionCable.server.config.logger
    ActionCable.server.config.logger = Logger.new(File::NULL)
    peer = Thread.new do
      socket = listener.accept
      socket.gets
      headers = {}
      while (line = socket.gets) && line != "\r\n"
        key, value = line.split(":", 2)
        headers[key.downcase] = value.strip
      end
      captured << headers
      socket.close
    end
    bus = LaravelCloud::Websockets::Bus.new {}
    captured.pop(timeout: 2)
  ensure
    bus&.close
    listener&.close
    peer&.join(2)
    ActionCable.server.config.logger = old_logger
  end

  def capture_publish(status: 200)
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
      socket.write("HTTP/1.1 #{status} Response\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
      socket.close
    end
    yield
    captured.pop(timeout: 2)
  ensure
    listener&.close
    thread&.join(2)
  end
end
