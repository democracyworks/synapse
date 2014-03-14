require 'socket'
require 'rubygems'
require 'eventmachine'

module Synapse
  module UDPServer
    def receive_data(data)
      to_ip = Thread.current.thread_variable_get(:to_ip)
      to_port = Thread.current.thread_variable_get(:to_port)
      relay = UDPSocket.new
      relay.connect(to_ip, to_port)
      relay.send(data, 0)
    end
  end

  class UDPForwarder
    def initialize(from_port)
      @from_port = from_port
      @thread = nil
    end

    def update(backends)
      return if backends.empty?
      backend = backends.shuffle.first
      to_ip = backend['host']
      to_port = backend['port']

      if @thread
        Thread.current.thread_variable_set(:to_ip, to_ip)
        Thread.current.thread_variable_set(:to_port, to_port)
      else
        @thread = run(to_ip, to_port)
      end
    end


    def run(to_ip, to_port)
      Thread.new {
        Thread.current.thread_variable_set(:to_ip, to_ip)
        Thread.current.thread_variable_set(:to_port, to_port)
        EM.run do
          EM.open_datagram_socket('localhost', @from_port, UPDServer)
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
        forwarder = @forwarders[watcher.name] || UDPForwarder.new(watcher.udp_forwarding['port'])
        forwarder.update(watcher.backends)
      end
    end
  end
end
end
