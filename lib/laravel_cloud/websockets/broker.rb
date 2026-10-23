module LaravelCloud
  module Websockets
    class Broker
      APPEND = <<~LUA.freeze
        local epoch = redis.call('HGET', KEYS[1], 'epoch')
        if not epoch then
          redis.call('DEL', KEYS[2])
          epoch = ARGV[1]
          redis.call('HSET', KEYS[1], 'epoch', epoch, 'offset', 0)
        end
        local offset = redis.call('HINCRBY', KEYS[1], 'offset', 1)
        local time = redis.call('TIME')
        local entry = cjson.encode({epoch=epoch, offset=offset, time=tonumber(time[1]), payload=ARGV[2], stream_id=ARGV[3]})
        redis.call('RPUSH', KEYS[2], entry)
        redis.call('LTRIM', KEYS[2], -100, -1)
        redis.call('EXPIRE', KEYS[1], 300)
        redis.call('EXPIRE', KEYS[2], 300)
        return entry
      LUA

      SNAPSHOT = <<~LUA.freeze
        return {redis.call('HGET', KEYS[1], 'epoch') or '', redis.call('HGET', KEYS[1], 'offset') or '0', redis.call('LRANGE', KEYS[2], 0, -1)}
      LUA

      def self.resolve
        unless defined?(::AnyCable)
          url = ENV["REDIS_URL"]
          return if url.to_s.empty?
          require "redis"
          options = { url: url }
          options[:ssl_params] = { verify_mode: OpenSSL::SSL::VERIFY_PEER } if url.start_with?("rediss://")
          return new(::Redis.new(**options))
        end
        if ::AnyCable.instance_variable_defined?(:@broadcast_adapter)
          adapter = ::AnyCable.broadcast_adapter
          return new(adapter.redis_conn) if adapter.respond_to?(:redis_conn)
        end
        config = ::AnyCable.config
        trace = config.to_source_trace || {}
        source = trace.dig("redis_url", :source, :type)
        adapter_source = trace.dig("broadcast_adapter", :source, :type)
        configured = ENV["REDIS_URL"].to_s != "" || ENV["ANYCABLE_REDIS_URL"].to_s != "" ||
          (source && source != :defaults) ||
          (%w[redis redisx].include?(config.broadcast_adapter.to_s) && adapter_source && adapter_source != :defaults)
        return unless configured
        require "redis"
        options = config.to_redis_params
        if config.redis_url.start_with?("rediss://") && [nil, :defaults].include?(trace.dig("redis_tls_verify", :source, :type))
          options[:ssl_params] = options.fetch(:ssl_params, {}).merge(verify_mode: OpenSSL::SSL::VERIFY_PEER)
        end
        new(::Redis.new(**options))
      end

      def initialize(connection)
        @connection = connection
      end

      def stream_id(stream)
        Digest::SHA256.hexdigest(stream)
      end

      def append(stream, payload)
        envelope = { epoch: "0" * 32, offset: 9_007_199_254_740_991, time: Time.now.to_i, payload: payload, stream_id: stream_id(stream) }
        Websockets.event_body(Websockets.channel(stream), "cloud-stream", envelope)
        JSON.parse(evaluate(APPEND, stream, SecureRandom.hex(16), payload, stream_id(stream)))
      end

      def snapshot(stream)
        epoch, offset, records = evaluate(SNAPSHOT, stream)
        { "epoch" => epoch, "offset" => offset.to_i, "entries" => records.map { |record| JSON.parse(record) } }
      end

      def read(stream, position = {}, since: nil)
        state = snapshot(stream)
        entries = state.fetch("entries")
        offset = position.fetch("offset", 0)
        epoch = position["epoch"]
        return if epoch && epoch != state.fetch("epoch")
        return if offset > state.fetch("offset")
        return if since && state.fetch("epoch").empty?
        return if entries.empty? && offset < state.fetch("offset")
        return if since && offset == 0 && since < Time.now.to_i - 300
        return if offset > 0 && (entries.empty? || offset < entries.first.fetch("offset") - 1)
        return if offset == 0 && !entries.empty? && entries.first.fetch("offset") > 1 && (!since || since <= entries.first.fetch("time"))
        entries.select { |entry| entry.fetch("offset") > offset && (!since || offset > 0 || entry.fetch("time") >= since) }
      end

      private

      def keys(stream)
        prefix = "roc:broker:{#{Digest::SHA256.hexdigest("#{ENV.fetch('PUSHER_APP_ID')}:#{stream}")}}"
        ["#{prefix}:state", "#{prefix}:history"]
      end

      def evaluate(script, stream, *arguments)
        @connection.eval(script, keys: keys(stream), argv: arguments)
      rescue StandardError => error
        raise Unavailable, "Configured AnyCable broker unavailable (#{error.class})"
      end
    end

    class StreamMessage < String
      attr_reader :position

      def initialize(entry)
        super(entry.fetch("payload"))
        @position = entry.slice("stream_id", "epoch", "offset")
      end
    end

    module StreamPositions
      private

      def worker_pool_stream_handler(broadcasting, user_handler, coder: nil)
        return super unless connection.respond_to?(:cloud_session)
        handler = stream_handler(broadcasting, user_handler, coder: coder)
        (@_cloud_stream_handlers ||= {})[broadcasting] = handler
        ->(message) do
          connection.worker_pool.work(connection) do
            connection.cloud_session.dispatch_stream(identifier, message) { handler.call(message) }
          end
        end
      end

      def cloud_replay(stream, entry)
        message = StreamMessage.new(entry)
        connection.cloud_session.dispatch_stream(identifier, message) { @_cloud_stream_handlers.fetch(stream).call(message) }
      end

      public

      def stop_stream_from(stream)
        @_cloud_stream_handlers&.delete(stream)
        super
      end

      def stop_all_streams
        @_cloud_stream_handlers&.clear
        super
      end
    end
  end
end
