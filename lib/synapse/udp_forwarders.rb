require 'socket'
require 'rubygems'
require 'eventmachine'

module Synapse
  class UDPServer < EM::Connection
    def initialize(remote)
      @remote = remote
    end

    def receive_data(data)
      host, port = @remote.get
      puts "Got data, forward to #{host}:#{port}"
      relay = UDPSocket.new
      relay.send(data, 0, host, port)
    end
  end

  class Remote
    attr_reader :host, :port

    def initialize(host, port)
      @host = host
      @port = port
      @mutex = Mutex.new
    end

    def get
      hostt = portt = nil
      @mutex.synchronize do
        hostt = host
        portt = port
      end
      [hostt,portt]
    end

    def update(host, port)
      @mutex.synchronize do
        @host = host
        @port = port
      end
    end
  end

  class UDPForwarder
    def initialize(from_port)
      @from_port = from_port
      @thread = nil
      @remote = nil
    end

    def update(backends)
      return if backends.empty?
      backend = backends.shuffle.first
      host = backend['host']
      port = backend['port']

      if @thread
        @remote.update(host, port)
      else
        @remote = Remote.new(host, port)
        @thread = run
      end
    end


    def run
      Thread.new {
        puts "Starting udp_forwarder for localhost:#{@from_port} -> #{@remote.host}:#{@remote.port}"
        EM.run do
          EM.open_datagram_socket('localhost', @from_port, UDPServer, @remote)
        end
      }
    end
  end

  class UDPForwarders
    def initialize
      @forwarders = {}
    end

    def update_config(watchers)
      watchers.each do |watcher|
        unless @forwarders[watcher.name]
          @forwarders[watcher.name] = UDPForwarder.new(watcher.udp_forwarding['port'])
        end
        @forwarders[watcher.name].update(watcher.backends)
      end
    end
  end
end
