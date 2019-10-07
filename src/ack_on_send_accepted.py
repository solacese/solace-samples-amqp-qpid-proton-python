#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from __future__ import print_function
import optparse
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import Message, Url

# helper function
def get_options():
    parser = optparse.OptionParser(usage="usage: %prog [options]",
                             description="Send messages on receipt of a message.")
    parser.add_option("-u", "--url", default="localhost:5672",
                    help="amqp message broker host url (default %default)")
    parser.add_option("-r", "--receive_destination", default="queue_test",
                   help="node address to which messages are sent (default %default)")
    parser.add_option("-s", "--send_destination", default="topic://send_topic",
                    help="number of messages to send (default %default)")
    parser.add_option("-o", "--username", default=None,
                    help="username for authentication (default %default)")
    parser.add_option("-p", "--password", default=None,
                    help="password for authentication (default %default)")
    parser.add_option("-q", "--qos", default="non-persistent",
                    help="Selects the message QoS for published messages. Valid values are [persistent or 2] for persistent messages. Valid values are [non-persistent or 1] for non-persistent messages. (default %default)" )
    opts, args = parser.parse_args()
    return opts


"""
    Proton acknowledge on send accepted class.
    This class sends a message to the provided send_address after receiving a message,
    and only acknowledges the received message once the sent message has been accepted 
    by the message broker.

    This example assumes that your Maximum Delivered Unacknowledged Messages per Flow is set to 1.

    Example cURL request that creates a queue with these settings:
    
    curl \
      --user <broker_admin_username>:<broker_admin_password> \
      -H "Content-Type: application/json" \
      -d '{"queueName":"<queue_name>", "maxDeliveredUnackedMsgsPerFlow": 1 }' \
      -X POST <broker_ip>:8080/SEMP/v2/config/msgVpns/<msgVpn>/queues"

    https://docs.solace.com/API-Developer-Online-Ref-Documentation/swagger-ui/config/index.html#/queue/createMsgVpnQueue
"""
class AckOnSendAccepted(MessagingHandler):

    def __init__(self, url,  username, password, send_destination, receive_destination):
        super(AckOnSendAccepted, self).__init__(auto_accept=False)

        # amqp broker host url
        self.url = url

        # authentication credentials
        self.username = username
        self.password = password

        # message counters
        self.sentMessages = 0
        self.receivedMessages = 0
        self.acceptedMessages = 0

        # destinations
        self.send_destination = send_destination
        self.receive_destination = receive_destination

        # message reference
        self.received_message = None
        
        self.receiver = None
      

    def on_start(self, event):
         # select authentication options for connection
        if self.username:
            # basic username and password authentication
            self.conn = event.container.connect(url=self.url, 
                                           user=self.username, 
                                           password=self.password, 
                                           allow_insecure_mechs=True)
        else:
            # Anonymous authentication
            self.conn = event.container.connect(url=self.url)
        
        # create receiver link to consume messages
        if self.conn:
            print("Connected to " + self.url)
            self.sender = event.container.create_sender(self.conn, None)

    def on_message(self, event):
        self.receivedMessages = self.receivedMessages + 1
        print("Received "  + str(self.receivedMessages) + " messages")
        self.received_message = event
        self.sender.send(Message(address=self.send_destination, body="RECEIVED"))
    
    def on_accepted(self, event):
        self.acceptedMessages = self.acceptedMessages + 1
        print("Sent " + str(self.acceptedMessages) + " messages")
        self.accept(self.received_message)
        self.received_message = None

     # Called once the sender has been created
    def on_link_opened(self, event):
        if event.link.is_sender:
            self.receiver = event.container.create_receiver(self.conn, self.receive_destination)
    
    # the on_transport_error event catches socket and authentication failures
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)
        

    def on_disconnected(self, event):
        print("Disconnected")
    
# parse arguments and get options
opts = get_options()

"""
The amqp address can be a topic or a queue.
Do not use a prefix or use 'queue://' in the amqp address for
the amqp receiver source address to receiver messages from a queue.
Use the prefix 'topic://' for a topic address
"""

try:
    Container(AckOnSendAccepted(opts.url, opts.username, opts.password, opts.send_destination, opts.receive_destination)).run()
except KeyboardInterrupt: pass
