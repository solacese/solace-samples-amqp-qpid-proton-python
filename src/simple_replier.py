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
    parser = optparse.OptionParser(usage="usage: %prog [options]")
    parser.add_option("-u", "--url", default="localhost:5672",
                  help="amqp message broker host url (default %default)")
    parser.add_option("-a", "--address", default="examples",
                  help="node address from which messages are received (default %default)")
    parser.add_option("-o", "--username", default=None,
                  help="username for authentication (default %default)")
    parser.add_option("-p", "--password", default=None,
                  help="password for authentication (default %default)")

    opts, args = parser.parse_args()

    return opts

"""
    Proton SimpleReplier class
    Creates an AMQP connection using ANONYMOUS or PLAIN authentication
    Then attaches a sender link to the broker after which it attaches a receiver link
"""
class SimpleReplier(MessagingHandler):
    def __init__(self, url, address, username, password):
        super(SimpleReplier, self).__init__()

        # amqp broker host url
        self.url = url
        self.address = address

        #authentication credentials
        self.username = username
        self.password = password

        #receiver
        self.receiver = None

    def on_sendable(self, event):
        if self.receiver is None:
            print( 'Creating the receiver now that we have a sender' )
            self.receiver = event.container.create_receiver(self.conn, self.address)
            
    def on_connected(self, event):
        print('on_connected event ', event)

    def on_accepted(self, event):
        print('Sent message was accepted: ', event)
    
    def on_start(self, event):
        print('Listening on ', self.address)
        self.container = event.container
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
        
        if self.conn:
            print("Connected to " + self.url)
            self.sender = event.container.create_sender(self.conn, None)

    def on_message(self, event):
        print('Received', event.message)
        self.sender.send( Message(address=event.message.reply_to, body=event.message.body.upper(),
                            correlation_id=event.message.correlation_id))
    
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)
        MessagingHandler.on_transport_error(self, event)

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
    Container(SimpleReplier(opts.url, opts.address, opts.username, opts.password)).run()
except KeyboardInterrupt: pass