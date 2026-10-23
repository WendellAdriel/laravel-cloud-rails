module ActionCable
  module SubscriptionAdapter
    class LaravelCloud < Base
      def broadcast(channel, payload)
        ::LaravelCloud::Websockets.broadcast(channel, payload)
      end

      def subscribe(*)
        raise ::LaravelCloud::Websockets::UnsupportedFeature, "Native WebSocket connections bypass the Cloud bridge"
      end

      def unsubscribe(*)
      end

      def shutdown
      end
    end
  end
end
