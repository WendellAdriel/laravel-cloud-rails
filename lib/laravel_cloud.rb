# frozen_string_literal: true

require "active_job"

require_relative "laravel_cloud/version"

# Laravel Cloud features for Rails. This module holds what features share (the
# dashboard event socket); each feature lives in its own namespace, starting
# with Managed Queues (LaravelCloud::Queue).
module LaravelCloud
  # Laravel's default too; LARAVEL_CLOUD_LOG_SOCKET overrides it.
  DEFAULT_LOG_SOCKET = "unix:///tmp/cloud-init.sock"

  class Error < StandardError; end

  class << self
    # The one dashboard event channel per process, shared by every feature.
    def events
      @events ||= Events.new(ENV.fetch("LARAVEL_CLOUD_LOG_SOCKET", DEFAULT_LOG_SOCKET))
    end

    attr_writer :events
  end
end

require_relative "laravel_cloud/events"
require_relative "laravel_cloud/queue"
require_relative "active_job/queue_adapters/cloud_adapter"

require_relative "laravel_cloud/websockets" if defined?(Rails::Railtie)
