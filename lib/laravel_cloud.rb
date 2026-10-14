# frozen_string_literal: true

require "active_job"

require_relative "laravel_cloud/version"

# Brings Laravel Cloud features to a Rails application.
#
# This module is a thin host: it owns the shared Laravel Cloud platform
# primitives — today the observability log socket and its {Events} emitter —
# and each capability lives in its own self-contained namespace that builds on
# them. Managed Queues ({LaravelCloud::Queue}) is the first feature; another
# capability slots in beside it the same way rather than adding host-level
# plumbing for one feature.
module LaravelCloud
  # The conventional address of the in-container observability log socket,
  # overridable at runtime via LARAVEL_CLOUD_LOG_SOCKET.
  DEFAULT_LOG_SOCKET = "unix:///tmp/cloud-init.sock"

  class Error < StandardError; end

  class << self
    # The shared emitter speaking Laravel Cloud's observability wire protocol.
    # Features surface dashboard metrics through this rather than opening their
    # own socket, so there is one channel per process.
    def events
      @events ||= Events.new(ENV.fetch("LARAVEL_CLOUD_LOG_SOCKET", DEFAULT_LOG_SOCKET))
    end

    attr_writer :events
  end
end

require_relative "laravel_cloud/events"
require_relative "laravel_cloud/queue"
require_relative "active_job/queue_adapters/cloud_adapter"

