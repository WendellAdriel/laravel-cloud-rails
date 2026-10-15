# frozen_string_literal: true

require "securerandom"

module LaravelCloud
  module Queue
    # Pushes jobs to SQS for the Active Job adapter.
    class Dispatcher
      # SQS limits: per-message delay, visibility timeout (a retry's backoff),
      # and SendMessageBatch size.
      MAX_DELAY_SECONDS = 900
      MAX_VISIBILITY_SECONDS = 43_200
      MAX_BATCH_MESSAGES = 10
      MAX_BATCH_BYTES = 1_048_576

      def initialize(runtime)
        @runtime = runtime
      end

      # `delay` is in seconds (a Float, from `wait:` / `wait_until:`).
      def dispatch(job, delay: 0)
        delivery = Delivery.current

        if delivery&.retry_of?(job)
          retry_job(job, delay, delivery)
        else
          push(job, queue_for(job), delay)
        end
      end

      # `ActiveJob.perform_all_later`. Every job is validated before anything is
      # sent, so an invalid one raises with nothing half-sent; a job SQS rejects
      # gets an `enqueue_error`. Returns the number enqueued.
      def dispatch_all(jobs)
        entries = jobs.map do |job|
          queue = queue_for(job)
          [job, queue, message_for(job, queue, delay_for(job))]
        end

        entries.group_by { |_, queue, _| queue }.sum do |queue, queue_entries|
          queue_url = require_queue_url(queue)

          batches(queue_entries).sum { |batch| send_batch(queue, queue_url, batch) }
        end
      end

      private

      def config
        @runtime.config
      end

      # A `retry_on` re-enqueue of the running job. Rather than pushing a copy,
      # the original message is released for SQS to redeliver after the backoff:
      # no duplicate, backoffs up to 12 hours, and it works on FIFO queues.
      def retry_job(job, delay, delivery)
        # Round up: truncating a sub-second backoff to 0 would make a hot retry
        # loop. Long polynomial backoffs are clamped to the 12-hour ceiling.
        seconds = delay.to_f.ceil.clamp(0, MAX_VISIBILITY_SECONDS)

        # Recorded first, so a failed cross-queue push below still retries here.
        delivery.release!(seconds)
        job.provider_job_id = delivery.message_id

        # `retry_on ..., queue:` moves the job, which a release can't do: push
        # it there as a new message and delete the original. Comparing with the
        # queue name it arrived with means only an explicit move gets here.
        return if job.queue_name.to_s == delivery.job_queue_name.to_s

        push(job, queue_for(job), delay)
        delivery.supersede!
      end

      def push(job, queue, delay)
        result = send_message(queue, message_for(job, queue, delay))

        job.provider_job_id = result.message_id
      end

      def send_message(queue, message)
        queue_url = require_queue_url(queue)

        result = begin
          @runtime.sqs.send_message(message.merge(queue_url: queue_url))
        rescue StandardError => e
          raise ActiveJob::EnqueueError, "Failed to push to the Laravel Cloud managed queue \"#{queue}\": #{e.message}"
        end

        @runtime.observer.queued(queue)
        result
      end

      def send_batch(queue, queue_url, batch)
        by_id = batch.each_with_index.to_h { |(job, _, _), index| [index.to_s, job] }
        messages = batch.each_with_index.map { |(_, _, message), index| message.merge(id: index.to_s) }

        result = begin
          @runtime.sqs.send_message_batch(queue_url: queue_url, entries: messages)
        rescue StandardError => e
          error = ActiveJob::EnqueueError.new("Failed to push to the Laravel Cloud managed queue \"#{queue}\": #{e.message}")
          by_id.each_value { |job| reject(job, error) }
          return 0
        end

        result.successful.each do |entry|
          job = by_id.fetch(entry.id)
          job.provider_job_id = entry.message_id
          job.successfully_enqueued = true
          @runtime.observer.queued(queue)
        end

        result.failed.each do |entry|
          reject(by_id.fetch(entry.id), ActiveJob::EnqueueError.new(
            "The Laravel Cloud managed queue \"#{queue}\" rejected the job: #{entry.code} #{entry.message}",
          ))
        end

        result.successful.size
      end

      def reject(job, error)
        job.successfully_enqueued = false
        job.enqueue_error = error
      end

      # Split one queue's messages by SendMessageBatch's limits.
      def batches(entries)
        batches = []
        bytes = 0

        entries.each do |entry|
          size = entry.last[:message_body].bytesize

          if batches.empty? || batches.last.size >= MAX_BATCH_MESSAGES || bytes + size > MAX_BATCH_BYTES
            batches << []
            bytes = 0
          end

          batches.last << entry
          bytes += size
        end

        batches
      end

      def message_for(job, queue, delay)
        group = attribute(job, :cloud_message_group_id)
        deduplication = attribute(job, :cloud_message_deduplication_id)

        { message_body: Message.encode(job) }.merge(attributes_for(queue, delay, group: group, deduplication: deduplication))
      end

      # FIFO queues need a group and deduplication ID and reject delays.
      # Anything a queue can't honour raises rather than being dropped.
      def attributes_for(queue, delay, group:, deduplication:)
        if ManagedQueueConfig.fifo?(queue)
          if delay.to_f.positive?
            raise InvalidDispatch, "The FIFO queue \"#{queue}\" does not support a per-message delay; " \
                                   "remove the wait or dispatch to a standard queue."
          end

          # Laravel's SqsQueue defaults: one group per queue, unique dedupe IDs.
          { message_group_id: group || queue, message_deduplication_id: deduplication || SecureRandom.uuid }
        else
          # Deduplication is FIFO-only; it would silently do nothing here.
          unless deduplication.nil?
            raise InvalidDispatch, "A message deduplication ID was set for the non-FIFO queue \"#{queue}\"; " \
                                   "deduplication requires a \".fifo\" queue."
          end

          attributes = {}

          # On a standard queue a group is a fair-queues tenant key.
          attributes[:message_group_id] = group unless group.nil?

          if delay.to_f.positive?
            seconds = delay.to_f.ceil

            # Raise rather than quietly run the job early.
            if seconds > MAX_DELAY_SECONDS
              raise InvalidDispatch, "A delay of #{seconds} seconds exceeds the SQS maximum of #{MAX_DELAY_SECONDS} " \
                                     "seconds (15 minutes); SQS cannot delay a message beyond 15 minutes."
            end

            attributes[:delay_seconds] = seconds
          end

          attributes
        end
      end

      def delay_for(job)
        job.scheduled_at ? job.scheduled_at.to_f - Time.now.to_f : 0
      end

      def require_queue_url(queue)
        config.queue_url_for(queue) ||
          raise(NotConfigured, "No Laravel Cloud managed queue is configured (LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG is missing).")
      end

      def attribute(job, name)
        job.respond_to?(name) ? job.public_send(name) : nil
      end

      # Active Job's default queue maps to the environment's default managed
      # queue; any other name is the managed queue of that name.
      def queue_for(job)
        name = job.queue_name.to_s

        name == job.class.queue_name_from_part(job.class.default_queue_name) ? config.queue : name
      end
    end
  end
end
