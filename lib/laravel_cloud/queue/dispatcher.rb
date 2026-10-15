# frozen_string_literal: true

require "securerandom"

module LaravelCloud
  module Queue
    # The push side of Managed Queues, behind the Active Job adapter. Pushing
    # always goes straight to SQS, on Laravel Cloud and locally alike.
    class Dispatcher
      # SQS hard-caps a per-message delivery delay at 15 minutes.
      MAX_DELAY_SECONDS = 900

      # SQS hard-caps a visibility timeout — and so a retry's backoff — at 12 hours.
      MAX_VISIBILITY_SECONDS = 43_200

      # SendMessageBatch takes at most 10 messages, totalling at most 1 MiB.
      MAX_BATCH_MESSAGES = 10
      MAX_BATCH_BYTES = 1_048_576

      def initialize(runtime)
        @runtime = runtime
      end

      # Push `job` to its managed queue, `delay` seconds from now (a Float, as
      # Active Job computes it from `wait:` / `wait_until:`).
      def dispatch(job, delay: 0)
        delivery = Delivery.current

        if delivery&.retry_of?(job)
          retry_job(job, delay, delivery)
        else
          push(job, queue_for(job), delay)
        end
      end

      # Push many jobs with SendMessageBatch — `ActiveJob.perform_all_later`.
      # Returns how many were enqueued, and marks each job enqueued or not, as
      # Active Job expects of an adapter's enqueue_all.
      #
      # Every job is validated and encoded before anything is sent, so a
      # dispatch the queue model can't honour raises with nothing half-sent.
      # Past that, SQS accepts or rejects each message individually: a
      # rejection lands on that job's `enqueue_error`, as it would for
      # `perform_later`.
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

      # Active Job's retry re-enqueue of the job the worker is running. Instead
      # of pushing a copy, the original message is released back to SQS: the
      # worker resets its visibility once the job returns, so SQS redelivers the
      # same message after the backoff. No duplicate is created, the backoff can
      # run past the 15-minute send cap up to SQS's 12-hour visibility ceiling,
      # and it works unchanged on FIFO queues, which reject per-message delays.
      def retry_job(job, delay, delivery)
        # Visibility timeouts are whole seconds, so round the backoff UP:
        # truncating a sub-second backoff to 0 would redeliver immediately and
        # turn the backoff into a hot retry loop. (An explicit `wait: 0` still
        # releases with no backoff — that is the caller's choice, and the
        # attempts are still bounded by `retry_on`'s `attempts:`.) Then clamp to
        # the 12-hour ceiling, which a long polynomial backoff can exceed.
        seconds = delay.to_f.ceil.clamp(0, MAX_VISIBILITY_SECONDS)

        # Record the release before anything else, so it's the outcome if the
        # cross-queue push below fails: the job then retries in place rather
        # than its message being deleted with no retry anywhere.
        delivery.release!(seconds)
        job.provider_job_id = delivery.message_id

        # A retry that moves the job to another queue (`retry_on ..., queue:`)
        # can't be a release — that would keep it where it is — so it's pushed
        # there as a fresh message and the original is deleted. Attempts are
        # per message (see Message.decode), so it starts again at attempt 1. Comparing against the queue name the message
        # arrived with (not the queue it was received from) means only an
        # explicit move takes this path.
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

      # Split one queue's messages by SendMessageBatch's count and size limits.
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

      # A ".fifo" queue name marks a FIFO queue, which SQS treats specially: it
      # requires a message group ID, requires a deduplication ID (unless the
      # queue has content-based deduplication), and rejects the per-message
      # delivery delay standard queues allow. A delay or attribute the queue
      # model can't honour raises rather than being quietly dropped.
      def attributes_for(queue, delay, group:, deduplication:)
        if ManagedQueueConfig.fifo?(queue)
          if delay.to_f.positive?
            raise InvalidDispatch, "The FIFO queue \"#{queue}\" does not support a per-message delay; " \
                                   "remove the wait or dispatch to a standard queue."
          end

          # The group defaults to the queue name (all of its messages ordered
          # together) and the deduplication ID to a unique value so messages
          # aren't dropped as duplicates — Laravel's SqsQueue defaults.
          { message_group_id: group || queue, message_deduplication_id: deduplication || SecureRandom.uuid }
        else
          # Deduplication is a FIFO-only attribute; on a standard queue it would
          # silently do nothing.
          unless deduplication.nil?
            raise InvalidDispatch, "A message deduplication ID was set for the non-FIFO queue \"#{queue}\"; " \
                                   "deduplication requires a \".fifo\" queue."
          end

          attributes = {}

          # A message group on a standard queue enables SQS fair queues: a pure
          # tenant key (no ordering, no deduplication) that SQS uses to spread
          # consumer capacity fairly across groups.
          attributes[:message_group_id] = group unless group.nil?

          if delay.to_f.positive?
            seconds = delay.to_f.ceil

            # A longer delay is unsatisfiable, so fail loudly rather than
            # delivering after 15 minutes: a job that must run in an hour is a
            # bug if it runs in fifteen. (Retry backoff isn't bound by this — a
            # retry is a release, handled above.)
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

      # The managed queue a job is pushed to. Active Job always names a queue —
      # "default" unless the job says otherwise — so a job on Active Job's
      # default queue goes to the environment's default managed queue (Laravel's
      # "no ->onQueue()"), and any other queue name targets the managed queue
      # of that name.
      def queue_for(job)
        name = job.queue_name.to_s

        name == job.class.queue_name_from_part(job.class.default_queue_name) ? config.queue : name
      end
    end
  end
end
