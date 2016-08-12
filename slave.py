#
# Hydra Distributed Computing Framework
# Client-side (slave)
# Ben Bartlett
#

import os, sys, time
import shutil
import socket
import hashlib
import xmlrpclib
import zipfile
import multiprocessing as mp
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer

masterNodeIP = 'http://' + 'localhost'
endkey = "&endkey&"
checksumkey = "&checksum&"  # Key to initiate file check
checkRate = 0.05  # Rate at which servers check for new messages
chunkSize = 2 ** 16
msgSize = 4096
selfIP = socket.gethostbyname(socket.gethostname())
nodeID = 0


def startClient():
    '''Returns a Client() instance and the associated XMLRPC server.'''
    # Allow xmlrpclib to support really long integers. Some random bug I found.
    xmlrpclib.Marshaller.dispatch[type(0L)] = lambda _, v, w: w("<value><i8>%d</i8></value>" % v)
    # Initialize
    host = selfIP
    port = commsPort
    # Start function server
    server = SimpleXMLRPCServer((host, port), logRequests = True, allow_none = True)
    client = Client()
    server.register_instance(client)
    return client, server
    # server.serve_forever()


def shutdown():
    # Time-delayed global shutdown to prevent xmlrpclib errors when calling shutdown
    time.sleep(.5)
    print ""
    sys.stdout.write("Shutting down")
    sys.stdout.flush()
    for i in range(10):
        time.sleep(.25)
        sys.stdout.write(".")
        sys.stdout.flush()
    os._exit(0)


class Client(object):
    '''Client class to communicate with master node.'''

    def __init__(self):
        '''Connect to master node on main port'''
        # self.job       = self.getCurrentJob()
        self.status = "waiting"
        self.substatus = ""
        self.host = masterNodeIP
        self.port = commsPort
        self.fport = filePort

    def ping(self):
        return True

    def close(self):
        # self.s.close()
        exitThread = Thread(target = shutdown)
        exitThread.start()
        return True


def runFunctionServer(quiet = True):
    FS = functionServer(quiet = quiet)
    FSserver = SimpleXMLRPCServer((FS.host, FS.port), logRequests = True, allow_none = True)
    FSserver.register_instance(FS)
    FSserver.serve_forever()


class functionServer(object):
    '''Creates a remote function server to allow functions to be called from the master node
    and executed on all of the child nodes.'''

    def __init__(self, quiet = True):
        # Initially import functions library
        # from functions import functions
        # self.functions = functions
        # Allow xmlrpclib to support really long integers. Some random bug I found.
        xmlrpclib.Marshaller.dispatch[type(0L)] = lambda _, v, w: w("<value><i8>%d</i8></value>" % v)
        # Initialize
        self.host = selfIP
        self.port = xmlrpcPort

        if not quiet: print "Function server at {} on port {}.".format(self.host, self.port)

    def dispatcher(self, functionName, args = None):
        '''Given functionName as a string, return the corresponding function.
        Allows for dynamic function registration.'''
        if args == None:
            return getattr(self.functions, functionName)()
        else:
            return getattr(self.functions, functionName)(*args)

    def refreshFunctions(self):
        '''Registers all top-level functions in the code you provide it.'''
        self.functions = reload(self.functions)
        ready = 0
        return ready
        # fnList = [f for _, f in self.functions.__dict__.iteritems() if callable(f)]
        # for f in fnList:
        #     self.server.register_function(f)


if __name__ == '__main__':
    # Node ID retrival
    nodeLocation = str(masterNodeIP) + ":20000"
    nodeServer = xmlrpclib.ServerProxy(nodeLocation)
    nodeID = nodeServer.assignID('http://' + str(selfIP))
    # nodeID = 0
    commsPort = 17000 + nodeID
    filePort = 18000 + nodeID
    xmlrpcPort = 19000 + nodeID

    # Start function server in separate process
    # functionServer()
    functionServerThread = Thread(target = runFunctionServer, args = (False,))
    functionServerThread.daemon = True
    functionServerThread.start()

    # Start comms server in separate process
    client, commsServer = startClient()
    nodeServer.nodeReady(nodeID)
    commsServer.serve_forever()

    # clientServerThread = Thread(target = startClient)
    # clientServerThread.daemon = True
    # clientServerThread.start()

    while True:  # Loop forever to keep daemon threads alive but be able to exit with ^C
        time.sleep(2)


        # client = Client(host, port)
        # client.close()
