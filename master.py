import os
import sys
import shutil
import pickle
import zipfile
import time
import cmd
import socket
import xmlrpclib
import subprocess
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer


class Server(object):
    '''Single instance of a server to communicate with a single node.'''

    def __init__(self, host, nodeID):
        '''Initiate connection with node on main port'''
        self.job = ""
        self.host = host
        self.port = nodeID + 17000
        self.xmlport = nodeID + 19000
        self.client = xmlrpclib.ServerProxy(self.host + ":" + str(self.port))
        self.fnServer = xmlrpclib.ServerProxy(self.host + ":" + str(self.xmlport))

    def pushCurrentJob(self):
        '''Tells the client nodes to request the current job.'''
        self.client.updateJob(self.job)
        ready = self.client.receiveCurrentJob()
        return ready

    def sendCurrentJob(self):
        '''Send the current queued job to client nodes.'''
        with open("queue/" + self.job + ".hj", "rb") as handle:
            return xmlrpclib.Binary(handle.read())

    def refreshFunctionServer(self):
        '''Tell function server to refresh contents.'''
        ready = self.fnServer.refreshFunctions()
        return ready


def startNodeServer(NS):
    xmlNS = SimpleXMLRPCServer(('localhost', 20000), logRequests = False, allow_none = True)
    xmlNS.register_instance(NS)
    xmlNS.serve_forever()


class nodeServer(object):
    '''Simple XMLRPC server that assigns node IDs when first connecting new nodes
    and tracks currently connected nodes.'''

    def __init__(self, quiet = True):
        # Allow xmlrpclib to support really long integers. Some random bug I found.
        self.maxNodes = 1024
        self.quiet = quiet
        self.addresses = [""] * self.maxNodes
        self.nodes = [-1] * self.maxNodes  # Global array representing which nodes are connected
        self.servers = [None] * self.maxNodes  # Actual array of server objects
        self.processes = [None] * self.maxNodes  # Array of server processes
        self.nodeIDs = range(0, self.maxNodes)

    def assignID(self, IP):
        '''Gives the client a port in exchange for their IP'''
        index = self.nodes.index(-1)
        ID = self.nodeIDs[index]
        self.nodes[index] = ID
        self.addresses[index] = str(IP)
        pickle.dump(self.nodes, open("nodes.dat", "wb"))  # Save nodes array to a file accessable by CFQueue
        pickle.dump(self.addresses, open("addresses.dat", "wb"))
        # if not self.quiet: print "Added node at {} with nodeID {}.".format(IP, ID)
        return ID

    def nodeReady(self, nodeID):
        '''Tells master node that a client node is ready and starts corresponding Server() process'''
        time.sleep(1)  # Just to make sure the client is started
        server = Server(self.addresses[nodeID], self.nodes[nodeID])
        self.servers[nodeID] = server
        if not self.quiet:
            print ""
            print "-> {:8}: Automatically added node {} at {}:" \
                .format(time.strftime('%X'), nodeID, self.addresses[nodeID].split("http://")[1])
            print " " * (5 + 8) + "Node object: {}".format(server)
            sys.stdout.write(">> ")  # Print new input character
            sys.stdout.flush()

    def distributeNodeInfo(self):
        '''Distributes current node information to objects wanting to use it, like distributed queues.'''
        return self.addresses, self.nodes

    def printInfo(self):
        print self.addresses
        print self.nodes
        print self.servers
        print self.processes


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

