# frozen_string_literal: true

require "json"
require "net/http"
require "uri"

module LaravelCloud
  module Queue
    # Retries a failed job from the Laravel Cloud dashboard — what
    # `laravel-cloud queue:retry <url>` runs.
    #
    # Laravel Cloud keeps failed jobs (the "failed_job" events the worker
    # emits) outside the app. To retry one it hands over an https:// URL whose
    # body is that failed job, encrypted with the application key. Mirroring
    # Laravel's Cloud FailedJobProvider: fetch, decrypt, push the original
    # payload back onto its queue, and emit a "failed_job" event carrying
    # "retried_at" so the dashboard marks it retried.
    class FailedJobRetrier
      class Error < LaravelCloud::Error; end

      FETCH_ATTEMPTS = 3
      FETCH_ERRORS = [SystemCallError, SocketError, IOError, Timeout::Error, OpenSSL::SSL::SSLError].freeze

      # `fetcher` returns the encrypted body for a URL; overridable for tests.
      def initialize(runtime: Queue.runtime, app_key: ENV["APP_KEY"], fetcher: nil)
        @runtime = runtime
        @app_key = app_key
        @fetcher = fetcher || method(:fetch)
      end

      # Returns the failed job that was retried.
      def retry(url)
        raise Error, "Failed job URLs must start with https://." unless url.to_s.start_with?("https://")
        raise Error, "An APP_KEY is required to decrypt failed jobs." if @app_key.to_s.empty?

        failed_job = decrypt(@fetcher.call(url))
        ensure_runnable!(failed_job)

        # Pushed verbatim: the payload is the message body exactly as it was
        # first queued, so the job starts over with a fresh delivery count.
        @runtime.dispatcher.push_body(failed_job["queue"], failed_job["payload"])

        @runtime.observer.retried(failed_job["id"], failed_job["queue"])

        failed_job
      end

      private

      def decrypt(body)
        failed_job = JSON.parse(LaravelEncrypter.new(@app_key).decrypt_string(body))

        unless failed_job.is_a?(Hash) && failed_job["payload"].is_a?(String) && failed_job["queue"].is_a?(String)
          raise Error, "The failed job is missing its queue or payload."
        end

        failed_job
      rescue JSON::ParserError
        raise Error, "The failed job is not valid JSON."
      rescue LaravelEncrypter::DecryptError, ArgumentError => e
        raise Error, "Unable to decrypt the failed job: #{e.message}"
      end

      # A payload trimmed for the log collector's line limit (see Observer) no
      # longer holds the whole job, so it can't be rebuilt. Refuse rather than
      # push a message that can only fail again as undecodable.
      def ensure_runnable!(failed_job)
        payload = JSON.parse(failed_job["payload"])

        if payload.is_a?(Hash) && payload["data_truncated"]
          raise Error, "The failed job #{failed_job['id']} can't be retried: its payload was too large to be kept whole."
        end

        Message.decode(failed_job["payload"])
      rescue JSON::ParserError, Message::Undecodable
        raise Error, "The failed job #{failed_job['id']} can't be retried: its payload is not a managed queue job."
      end

      def fetch(url)
        uri = URI.parse(url)
        attempts = 0

        begin
          attempts += 1

          response = Net::HTTP.start(uri.host, uri.port, use_ssl: true, open_timeout: 10, read_timeout: 10) do |http|
            http.request(Net::HTTP::Get.new(uri))
          end
        rescue *FETCH_ERRORS => e
          raise Error, "Unable to fetch the failed job: #{e.message}" if attempts >= FETCH_ATTEMPTS

          sleep 1
          retry
        end

        raise Error, "Unable to fetch the failed job (HTTP #{response.code})." unless response.is_a?(Net::HTTPSuccess)

        response.body
      end
    end
  end
end
