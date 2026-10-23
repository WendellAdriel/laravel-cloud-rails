module LaravelCloud
  module Websockets
    class Presence
      attr_reader :stream

      def initialize(session, identifier, stream)
        @session, @identifier, @stream = session, identifier, stream
        prefix = ActionCable.server.config.cable["channel_prefix"] || ActionCable.server.config.cable[:channel_prefix]
        @channel = "presence-roc-#{Digest::SHA256.hexdigest([prefix, stream].compact.join(":"))}"
        @member = Digest::SHA256.hexdigest("#{session.token}:#{identifier}")
        @lock = Monitor.new
        @records = {}
        @closed = false
        @generation = 0
        @bus = Bus.new(serial: true) { failed }
        update(nil)
      rescue StandardError
        close
        raise
      end

      def join(id, info)
        update({ "id" => id, "info" => info })
      end

      def leave(id = nil)
        return if id && id != @membership_id
        update(nil)
      end

      def info(return_records: true)
        @lock.synchronize do
          records = members.values
          { type: "info", total: records.size }.tap do |result|
            result[:records] = records if return_records
          end
        end
      end

      def ping
        @bus.ping
      end

      def close
        @lock.synchronize { @closed = true }
        @bus&.close
      end

      private

      def members
        @records.values.compact.to_h { |record| [record.fetch("id"), record] }
      end

      def update(record)
        changes = @lock.synchronize do
          before = members
          @membership_id = record&.fetch("id")
          @bus.unsubscribe(@channel, @callback)
          generation = (@generation += 1)
          @callback = ->(event, data) { receive(generation, event, data) }
          data = JSON.generate(user_id: @member, user_info: record)
          snapshot = @bus.subscribe(@channel, @callback, channel_data: data)
          @records = snapshot.fetch("presence").fetch("hash").transform_values { |value| value.is_a?(Hash) && value.key?("id") ? value : nil }
          transitions(before)
        end
        deliver(changes)
      end

      def receive(generation, event, data)
        changes = @lock.synchronize do
          return if @closed || generation != @generation
          before = members
          case event
          when "pusher_internal:member_added"
            value = data["user_info"]
            @records[data.fetch("user_id")] = value.is_a?(Hash) && value.key?("id") ? value : nil
          when "pusher_internal:member_removed"
            @records.delete(data.fetch("user_id"))
          else
            return
          end
          transitions(before)
        end
        deliver(changes)
      end

      def transitions(before)
        after = members
        (after.keys - before.keys).map { |id| after.fetch(id).merge("type" => "join") } +
          (before.keys - after.keys).map { |id| { "type" => "leave", "id" => id } }
      end

      def deliver(changes)
        changes.each { |message| @session.deliver(type: "presence", identifier: @identifier, message: message) }
      end

      def failed
        closed = @lock.synchronize { @closed }
        @session.close(reason: "presence_backend_unavailable") unless closed
      end
    end

    module ChannelPresence
      def join_presence(stream = nil, id: user_presence_id, info: user_presence_info)
        return super unless connection.respond_to?(:cloud_session)
        connection.cloud_session.join_presence(identifier, stream, id.to_s, info)
      end

      def leave_presence(id = user_presence_id)
        return super unless connection.respond_to?(:cloud_session)
        connection.cloud_session.leave_presence(identifier, id.to_s)
      end
    end
  end
end
