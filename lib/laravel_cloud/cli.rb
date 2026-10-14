# frozen_string_literal: true

require "optparse"

module LaravelCloud
  # The `laravel-cloud` command — the process Laravel Cloud runs for a Rails
  # app's queue workers and failed-job retries:
  #
  #   laravel-cloud queue:work [options]
  #   laravel-cloud queue:retry <failed-job-url>
  #
  # Run from the application root (`bundle exec laravel-cloud ...`): it boots
  # the app from config/environment.rb, as `bin/rails runner` would.
  class CLI
    COMMANDS = %w[queue:work queue:retry].freeze

    def initialize(argv, out: $stdout, err: $stderr, boot: true)
      @argv = argv.dup
      @out = out
      @err = err
      @boot = boot
    end

    # Returns the process exit status.
    def run
      command = @argv.shift

      case command
      when "queue:work" then work
      when "queue:retry" then retry_failed_job
      when "-v", "--version"
        @out.puts("laravel-cloud #{VERSION}")
        0
      else
        @err.puts(command.nil? || %w[-h --help].include?(command) ? usage : "Unknown command #{command.inspect}.\n\n#{usage}")
        command.nil? || %w[-h --help].include?(command) ? 0 : 1
      end
    rescue OptionParser::ParseError => e
      @err.puts("#{e.message}\n\n#{usage}")
      1
    rescue LaravelCloud::Error => e
      # Expected failures — an unreachable agent, a missing managed queue, a
      # failed job that can't be retried — exit non-zero with the reason, no
      # backtrace. On Cloud a non-zero worker exit gets the container restarted.
      @err.puts("[LaravelCloud] #{e.message}")
      1
    end

    private

    def work
      options = { queues: [] }

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: laravel-cloud queue:work [options]"
        opts.on("-q", "--queue NAME", "Queue(s) to poll without the Cloud agent, in order; repeat or comma-separate") do |value|
          options[:queues].concat(value.split(",").map(&:strip).reject(&:empty?))
        end
        opts.on("--tries N", Integer, "Times a job that raises is attempted in all (default 1)") { |value| options[:tries] = value }
        opts.on("--backoff SECONDS", Integer, "Wait before re-attempting a job that raised (default 0)") { |value| options[:backoff] = value }
        opts.on("--max-jobs N", Integer, "Stop after processing N jobs") { |value| options[:max_jobs] = value }
        opts.on("--max-time SECONDS", Integer, "Stop after running for this long") { |value| options[:max_time] = value }
        opts.on("--shutdown-timeout SECONDS", Integer, "Time a running job gets to finish on shutdown (default 15)") do |value|
          options[:shutdown_timeout] = value
        end
        opts.on("--stop-when-empty", "Exit once the queues are drained") { options[:stop_when_empty] = true }
      end

      parser.parse!(@argv)
      boot_app

      Queue::Worker.new(**options).run
      0
    end

    def retry_failed_job
      url = @argv.shift

      if url.nil? || url.empty?
        @err.puts("Usage: laravel-cloud queue:retry <failed-job-url>")
        return 1
      end

      boot_app

      failed_job = Queue::FailedJobRetrier.new.retry(url)
      @out.puts("[LaravelCloud] Retried failed job #{failed_job['id']} on queue #{failed_job['queue']}")
      0
    end

    def boot_app
      return unless @boot

      environment = File.expand_path("config/environment.rb")

      unless File.exist?(environment)
        raise LaravelCloud::Error, "No Rails application found: run laravel-cloud from the application root (#{environment} is missing)."
      end

      require environment

      # A long-lived worker wants the app fully loaded up front, as it would be
      # when serving requests; booting this way skips eager loading otherwise.
      Rails.application.eager_load! if Rails.application.config.eager_load
    end

    def usage
      <<~USAGE
        Usage: laravel-cloud <command> [options]

        Commands:
          queue:work                 Process Laravel Cloud managed queue jobs (--help for options)
          queue:retry <url>          Retry a failed job handed over by Laravel Cloud
      USAGE
    end
  end
end
