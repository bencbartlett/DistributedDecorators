import os
import sys
import shutil
import pickle
import zipfile
import time
import cmd
import socket
import xmlrpclib
import marshal
import subprocess
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer


def f(x):
    print x
    return x * 2


class Server(object):
    '''Single instance of a server to communicate with a single node.'''

    def __init__(self, host, port):
        '''Initiate connection with node on main port'''
        self.host = host
        # self.port = nodeID + 17000
        self.xmlport = port
        self.fnServer = xmlrpclib.ServerProxy(self.host + ":" + str(self.xmlport))

        # Comment this out
        self.distributeFunctions([f])

    def distributeFunctions(self, fnList):
        # Serialize the function code and send
        fnCodeDict = {fn.__name__: xmlrpclib.Binary(marshal.dumps(fn.func_code)) for fn in fnList}
        print fnCodeDict
        self.fnServer.receiveFunctions(fnCodeDict)


def startNodeServer(NS):
    xmlNS = SimpleXMLRPCServer(('localhost', 20000), logRequests = False, allow_none = True)
    xmlNS.register_instance(NS)
    xmlNS.serve_forever()


class nodeServer(object):
    '''Simple XMLRPC server that assigns node IDs when first connecting new nodes
    and tracks currently connected nodes.'''

    def __init__(self, quiet = True):
        # Allow xmlrpclib to support really long integers. Some random bug I found.
        self.quiet = quiet
        self.nodes = {}
        self.servers = {}
        self.currentNodeID = 0

    def addNode(self, IP):
        '''Gives the client a port in exchange for their IP'''
        node = {"ID": str(self.currentNodeID),
                "IP": IP}
        # Default configuration is ports 19000 and up
        node["port"] = 19000 + int(node["ID"])
        self.nodes[node["ID"]] = node
        self.currentNodeID += 1
        # pickle.dump(self.nodes, open("nodes.dat", "wb"))  # Save nodes array to a file accessable by CFQueue
        # pickle.dump(self.addresses, open("addresses.dat", "wb"))
        if not self.quiet: print "Added node at {} with nodeID {}.".format(node["IP"], node["ID"])
        return node["ID"]

    def nodeReady(self, nodeID):
        '''Tells master node that a client node is ready and starts corresponding Server() process'''
        node = self.nodes[str(nodeID)]
        server = Server(node["IP"], node["port"])
        # Add server to node object
        self.servers[node["ID"]] = server
        if not self.quiet:
            print ""
            print "-> {:8}: Automatically added node {} at {}:" \
                .format(time.strftime('%X'), node["ID"], node["IP"].split("http://")[1])
            print " " * (5 + 8) + "Node object: {}".format(server)
            sys.stdout.write(">> ")  # Print new input character
            sys.stdout.flush()

    def distributeNodeInfo(self):
        '''Distributes current node information to objects wanting to use it, like distributed queues.'''
        print self.nodes
        return self.nodes


if __name__ == '__main__':
    # Start node server in separate thread
    NS = nodeServer(quiet = False)
    nodeServerThread = Thread(target = startNodeServer, args = (NS,))
    nodeServerThread.daemon = True
    nodeServerThread.start()

    # Start master server in a separate thread
    # MS = masterServer(NS, quiet = False)


    while True:  # Loop forever to keep daemon threads alive but be able to exit with ^C
        time.sleep(2)
