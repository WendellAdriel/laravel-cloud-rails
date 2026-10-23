require "action_cable/subscription_adapter/base"

module ActionCable
  module SubscriptionAdapter
    class LaravelCloud < Base
      def broadcast(channel, payload)
        ::LaravelCloud::Websockets.publish(::LaravelCloud::Websockets.channel(channel), "cloud-broadcast", payload)
      end

      def subscribe(channel, callback, success_callback = nil)
        runtime = ::LaravelCloud::Websockets.runtime
        runtime.subscribe(channel, callback, success_callback)
      end

      def unsubscribe(channel, callback)
        ::LaravelCloud::Websockets.runtime.unsubscribe(channel, callback)
      end

      def shutdown
        ::LaravelCloud::Websockets.runtime.stop
      end
    end
  end
end
