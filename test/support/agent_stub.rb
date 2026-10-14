# frozen_string_literal: true

require "socket"

# A minimal HTTP/1.1 server over a unix socket, standing in for the
# in-container cloud-agent so AgentClient's real Net::HTTP-over-unix-socket path
# is exercised end to end.
#
# GET /next serves the queued messages in order, then 204. POST /result
# answers 200 (or the next scripted status) and records the reported payload.
class AgentStub
  attr_reader :path, :results, :requests

  def initialize(messages: [], result_statuses: [], next_status: nil)
    @dir = Dir.mktmpdir("agent")
    @path = File.join(@dir, "agent.sock")
    @messages = messages.dup
    @result_statuses = result_statuses.dup
    @next_status = next_status
    @results = []
    @requests = []
    @server = UNIXServer.new(@path)
    @thread = Thread.new { serve }
  end

  def close
    @thread.kill
    @server.close
    FileUtils.rm_rf(@dir)
  end

  private

  def serve
    loop do
      connection = @server.accept
      handle(connection)
    ensure
      connection&.close
    end
  end

  def handle(connection)
    request_line = connection.gets.to_s
    headers = {}
    while (line = connection.gets) && line != "\r\n"
      key, value = line.split(":", 2)
      headers[key.downcase] = value.strip
    end
    body = connection.read(headers["content-length"].to_i).to_s

    method, path = request_line.split
    @requests << "#{method} #{path}"

    status, response = if path == "/next"
      next_response
    else
      @results << JSON.parse(body)
      [@result_statuses.shift || 200, ""]
    end

    connection.write("HTTP/1.1 #{status} X\r\nContent-Type: application/json\r\nContent-Length: #{response.bytesize}\r\nConnection: close\r\n\r\n#{response}")
  end

  def next_response
    return [@next_status, ""] if @next_status

    message = @messages.shift
    message ? [200, JSON.generate(message)] : [204, ""]
  end
end
