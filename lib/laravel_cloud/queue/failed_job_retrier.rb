# frozen_string_literal: true

require "json"
require "net/http"
require "uri"

module LaravelCloud
  module Queue
    # Retries failed jobs from the Laravel Cloud dashboard — what
    # `laravel-cloud queue:retry <url>` runs.
    #
    # Laravel Cloud keeps failed jobs (the "failed_job" events the worker
    # emits) outside the app, and hands the retry command an https:// URL to
    # fetch them from. This is a port of Laravel's Cloud FailedJobProvider
    # (find / forget) plus the queue:retry loop around it:
    #
    #   1. GET the URL, asking for payload version 1 and naming our cipher.
    #   2. Decrypt the body with the application key. Version 1 is a page —
    #      { data: [failed jobs], links: { self, next } } — and an unversioned
    #      response is a single failed job (the legacy format).
    #   3. For each failed job: push its payload back onto its queue, then emit
    #      a "failed_job" event with "retried_at" so the dashboard marks it
    #      retried. When a page runs out, follow links.next.
    class FailedJobRetrier
      class Error < LaravelCloud::Error; end

      PAYLOAD_VERSION = "1"

      # Laravel's Http::retry(10, ...) policy: up to 10 attempts, backing off
      # 500ms * 2^(attempt - 1) between them, and never retrying a 403 (an
      # expired or invalid signed URL won't get better).
      FETCH_ATTEMPTS = 10
      FETCH_TIMEOUT = 10
      FETCH_ERRORS = [SystemCallError, SocketError, IOError, Timeout::Error, OpenSSL::SSL::SSLError, EOFError].freeze

      # An HTTP response, reduced to what the retrier reads.
      Response = Struct.new(:status, :headers, :body) do
        def header(name)
          headers.find { |key, _| key.casecmp?(name) }&.last.to_s
        end
      end

      # The outcome of one failed job: retried, or skipped (left in the
      # dashboard) with the reason.
      Result = Struct.new(:id, :queue, :error) do
        def retried?
          error.nil?
        end
      end

      # `fetcher` takes (url, headers) and returns a Response; `sleeper` takes
      # seconds. Both are overridable for tests.
      def initialize(runtime: Queue.runtime, app_key: ENV["APP_KEY"], fetcher: nil, sleeper: nil)
        @runtime = runtime
        @app_key = app_key
        @fetcher = fetcher || method(:http_get)
        @sleeper = sleeper || ->(seconds) { sleep(seconds) }
      end

      # Retry every failed job the URL yields, calling the block with each
      # Result as it happens. Returns the Results.
      #
      # A failed job whose payload can't be rebuilt is skipped and stays in the
      # dashboard. Anything else going wrong — the fetch, decryption, a push —
      # raises Error, as Laravel's queue:retry stops at the first failure. Jobs
      # already retried stay retried; the one that failed isn't marked.
      def retry(url)
        raise Error, "Failed job URLs must start with https://." unless url.to_s.start_with?("https://")
        raise Error, "An APP_KEY is required to decrypt failed jobs." if @app_key.to_s.empty?

        encrypter = encrypter_for(@app_key)
        results = []

        each_failed_job(url, encrypter) do |failed_job|
          result = retry_failed_job(failed_job)
          results << result
          yield result if block_given?
        end

        results
      end

      private

      def retry_failed_job(failed_job)
        id = failed_job["id"].to_s
        queue = failed_job["queue"]
        payload = failed_job["payload"]

        unless queue.is_a?(String) && payload.is_a?(String)
          raise Error, "The failed job #{id} is missing its queue or payload."
        end

        unrebuildable = unrebuildable_reason(payload)
        return Result.new(id, queue, unrebuildable) if unrebuildable

        # Pushed verbatim: the payload is the message body exactly as it was
        # first queued, so the job starts over with a fresh delivery count.
        begin
          @runtime.dispatcher.push_body(queue, payload)
        rescue StandardError => e
          raise Error, "Unable to push the failed job #{id} back onto \"#{queue}\": #{e.message}"
        end

        # FailedJobProvider::forget — only once the push has succeeded.
        @runtime.observer.retried(id, queue)

        Result.new(id, queue, nil)
      end

      # A payload trimmed for the log collector's line limit (see Observer) no
      # longer holds the whole job, so it can't be rebuilt; pushing it would
      # only fail again as undecodable.
      def unrebuildable_reason(payload)
        decoded = JSON.parse(payload)

        return "its payload was too large to be kept whole" if decoded.is_a?(Hash) && decoded["data_truncated"]

        Message.decode(payload)
        nil
      rescue JSON::ParserError, Message::Undecodable
        "its payload is not a managed queue job"
      end

      # FailedJobProvider::failedJobsIterator: yield each failed job, fetching
      # the next page only once the current one is used up.
      def each_failed_job(url, encrypter)
        page = fetch_page(url, encrypter)

        loop do
          page[:data].each { |failed_job| yield failed_job }

          break if page[:next].nil?

          page = fetch_page(page[:next], encrypter)
        end
      end

      # FailedJobProvider::resolveFailedJobsPayload.
      def fetch_page(url, encrypter)
        response = fetch(url, encrypter.cipher)
        payload = decrypt(response.body, encrypter)

        case response.header("Cloud-Payload-Version")
        when PAYLOAD_VERSION
          data = payload.is_a?(Hash) ? payload["data"] : nil
          links = payload.is_a?(Hash) && payload["links"].is_a?(Hash) ? payload["links"] : {}
          raise Error, "The failed jobs page is missing its data." unless data.is_a?(Array)

          { data: data, next: presence(links["next"]) }
        when ""
          { data: [payload], next: nil }
        else
          raise Error, "Unsupported payload version: #{response.header('Cloud-Payload-Version')}"
        end
      end

      def decrypt(body, encrypter)
        JSON.parse(encrypter.decrypt_string(body))
      rescue JSON::ParserError
        raise Error, "The failed jobs payload is not valid JSON."
      rescue LaravelEncrypter::DecryptError => e
        raise Error, "Unable to decrypt the failed jobs: #{e.message}"
      end

      # FailedJobProvider::fetchFailedJobs, with Laravel's retry policy.
      def fetch(url, cipher)
        headers = { "Cloud-Payload-Version" => PAYLOAD_VERSION, "Cloud-Encryption-Cipher" => cipher }
        attempt = 0

        loop do
          attempt += 1

          begin
            response = @fetcher.call(url, headers)
          rescue *FETCH_ERRORS => e
            raise Error, "Unable to fetch the failed jobs: #{e.message}" if attempt >= FETCH_ATTEMPTS
          else
            return response if (200..299).cover?(response.status)

            if response.status == 403 || attempt >= FETCH_ATTEMPTS
              raise Error, "Unable to fetch the failed jobs (HTTP #{response.status})."
            end
          end

          @sleeper.call(0.5 * (2**(attempt - 1)))
        end
      end

      def http_get(url, headers)
        uri = URI.parse(url)

        Net::HTTP.start(uri.host, uri.port, use_ssl: true, open_timeout: FETCH_TIMEOUT, read_timeout: FETCH_TIMEOUT) do |http|
          request = Net::HTTP::Get.new(uri)
          headers.each { |name, value| request[name] = value }

          response = http.request(request)
          Response.new(response.code.to_i, response.each_header.to_a, response.body.to_s)
        end
      end

      def encrypter_for(app_key)
        LaravelEncrypter.new(app_key)
      rescue ArgumentError => e
        raise Error, "The APP_KEY can't be used to decrypt failed jobs: #{e.message}"
      end

      def presence(value)
        value.is_a?(String) && !value.empty? ? value : nil
      end
    end
  end
end
