require "action_cable/subscription_adapter/base"
require "action_cable/subscription_adapter/channel_prefix"

module ActionCable
  module SubscriptionAdapter
    class LaravelCloud < Base
      prepend ChannelPrefix

      def initialize(server)
        super
        @bindings = {}
        @bindings_lock = Mutex.new
      end

      def broadcast(channel, payload)
        broker = ::LaravelCloud::Websockets.broker
        event, data = broker ? ["cloud-stream", broker.append(channel, payload)] : ["cloud-broadcast", payload]
        ::LaravelCloud::Websockets.publish(::LaravelCloud::Websockets.channel(channel), event, data)
      end

      def subscribe(channel, callback, success_callback = nil)
        runtime = ::LaravelCloud::Websockets.runtime
        @bindings_lock.synchronize { @bindings[[channel, callback]] = runtime }
        runtime.subscribe(channel, callback, success_callback)
      end

      def unsubscribe(channel, callback)
        runtime = @bindings_lock.synchronize { @bindings.delete([channel, callback]) }
        runtime&.unsubscribe(channel, callback)
      end

      def shutdown
        runtimes = @bindings_lock.synchronize { @bindings.values.uniq }
        runtimes.each(&:stop)
      end
    end
  end
end
