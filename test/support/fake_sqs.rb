# frozen_string_literal: true

# A stateful in-memory SQS behind the real AWS SDK client (stub_responses), so
# tests drive the actual SQS operations and parameter validation the code uses.
#
# Messages become invisible when received and visible again on
# ChangeMessageVisibility (the requested timeout is recorded, not waited out),
# and each receive bumps ApproximateReceiveCount exactly as SQS does.
class FakeSqs
  Message = Struct.new(:message_id, :queue_url, :body, :receive_count, :receipt_handle, :visible, :params)

  attr_reader :client, :sent, :batches, :deleted, :visibility_changes

  # Batch entry bodies SQS should reject, as SendMessageBatch reports per entry.
  attr_accessor :reject_bodies_matching

  def initialize
    @client = Aws::SQS::Client.new(stub_responses: true, region: "us-east-2")
    @messages = []
    @sent = []
    @batches = []
    @reject_bodies_matching = nil
    @deleted = []
    @visibility_changes = []
    @sequence = 0

    @client.stub_responses(:send_message, ->(context) { send_message(context.params) })
    @client.stub_responses(:send_message_batch, ->(context) { send_message_batch(context.params) })
    @client.stub_responses(:receive_message, ->(context) { receive_message(context.params) })
    @client.stub_responses(:delete_message, ->(context) { delete_message(context.params) })
    @client.stub_responses(:change_message_visibility, ->(context) { change_visibility(context.params) })
  end

  # Put a raw body on a queue, as if something else had sent it.
  def inject(queue_url, body)
    send_message(queue_url: queue_url, message_body: body)
  end

  def pending(queue_url = nil)
    @messages.select { |message| queue_url.nil? || message.queue_url == queue_url }
  end

  private

  def send_message(params)
    @sequence += 1
    message = Message.new("msg-#{@sequence}", params[:queue_url], params[:message_body], 0, nil, true, params)
    @messages << message
    @sent << params

    { message_id: message.message_id }
  end

  def send_message_batch(params)
    @batches << params
    successful = []
    failed = []

    params[:entries].each do |entry|
      if reject_bodies_matching && entry[:message_body].match?(reject_bodies_matching)
        failed << { id: entry[:id], code: "InvalidParameterValue", message: "rejected", sender_fault: true }
      else
        message_id = send_message(entry.except(:id).merge(queue_url: params[:queue_url]))[:message_id]
        successful << { id: entry[:id], message_id: message_id, md5_of_message_body: "x" }
      end
    end

    { successful: successful, failed: failed }
  end

  def receive_message(params)
    message = @messages.find { |candidate| candidate.queue_url == params[:queue_url] && candidate.visible }
    return { messages: [] } if message.nil?

    message.receive_count += 1
    message.receipt_handle = "receipt-#{message.message_id}-#{message.receive_count}"
    message.visible = false

    {
      messages: [{
        message_id: message.message_id,
        receipt_handle: message.receipt_handle,
        body: message.body,
        attributes: { "ApproximateReceiveCount" => message.receive_count.to_s },
      }],
    }
  end

  def delete_message(params)
    @deleted << params
    @messages.reject! { |message| message.receipt_handle == params[:receipt_handle] }
    {}
  end

  def change_visibility(params)
    @visibility_changes << params
    message = @messages.find { |candidate| candidate.receipt_handle == params[:receipt_handle] }
    message.visible = true if message
    {}
  end
end
