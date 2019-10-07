#!/usr/bin/env python

from __future__ import print_function, unicode_literals
import optparse
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container, DynamicNodeProperties

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



class SimpleRequestor(MessagingHandler):
    def __init__(self, url, address, username,password, requests):
        super(SimpleRequestor, self).__init__()
        # amqp broker host url
        self.url = url
        self.address = address

        #authentication credentials
        self.username = username
        self.password = password

        #receiver
        self.receiver = None
        self.requests = requests

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

    def next_request(self):
        if self.receiver.remote_source.address:
            print("Doing request " + str(self.receiver.remote_source.address))
            req = Message(address=self.address,reply_to=self.receiver.remote_source.address, body=self.requests[0])
            self.sender.send(req)

    def on_link_opened(self, event):
        if self.receiver != None and event.receiver == self.receiver:
            self.next_request()
        elif self.receiver is None: 
            self.receiver = event.container.create_receiver(self.conn, None, dynamic=True)

    def on_message(self, event):
        print("%s => %s" % (self.requests.pop(0), event.message.body))
        if self.requests:
            self.next_request()
        else:
            event.connection.close()

REQUESTS= ["Twas brillig, and the slithy toves",
           "Did gire and gymble in the wabe.",
           "All mimsy were the borogroves,",
           "And the mome raths outgrabe."]

# parse arguments and get options
opts = get_options()

Container(SimpleRequestor(opts.url, opts.address, opts.username, opts.password, REQUESTS)).run()

