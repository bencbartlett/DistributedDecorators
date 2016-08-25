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
import marshal
import types
import zipfile
import multiprocessing as mp
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer

masterNodeIP = 'http://' + 'localhost'
# endkey = "&endkey&"
# checksumkey = "&checksum&"  # Key to initiate file check
# checkRate = 0.05  # Rate at which servers check for new messages
# chunkSize = 2 ** 16
# msgSize = 4096
selfIP = socket.gethostbyname(socket.gethostname())


# def startClient(host, port):
#     '''Returns a Client() instance and the associated XMLRPC server.'''
#     # Allow xmlrpclib to support really long integers. Some random bug I found.
#     xmlrpclib.Marshaller.dispatch[type(0L)] = lambda _, v, w: w("<value><i8>%d</i8></value>" % v)
#     # Start function server
#     server = SimpleXMLRPCServer((host, port), logRequests = True, allow_none = True)
#     client = Client()
#     server.register_instance(client)
#     return client, server
#     # server.serve_forever()


# def shutdown():
#     # Time-delayed global shutdown to prevent xmlrpclib errors when calling shutdown
#     time.sleep(.5)
#     print ""
#     sys.stdout.write("Shutting down")
#     sys.stdout.flush()
#     for i in range(10):
#         time.sleep(.25)
#         sys.stdout.write(".")
#         sys.stdout.flush()
#     os._exit(0)


# class Client(object):
#     '''Client class to communicate with master node.'''
#
#     def __init__(self):
#         '''Connect to master node on main port'''
#         # self.job       = self.getCurrentJob()
#         self.status = "waiting"
#         self.substatus = ""
#         self.host = masterNodeIP
#         self.port = commsPort
#
#     def ping(self):
#         return True
#
#     def close(self):
#         # self.s.close()
#         exitThread = Thread(target = shutdown)
#         exitThread.start()
#         return True


def runFunctionServer(port, quiet = True):
    FS = functionServer(port, quiet = quiet)
    FSserver = SimpleXMLRPCServer((FS.host, FS.port), logRequests = True, allow_none = True)
    FSserver.register_instance(FS)
    FSserver.serve_forever()


class functionServer(object):
    '''Creates a remote function server to allow functions to be called from the master node
    and executed on all of the child nodes.'''

    def __init__(self, port, quiet = True):
        # Allow xmlrpclib to support really long integers. Some random bug I found.
        xmlrpclib.Marshaller.dispatch[type(0L)] = lambda _, v, w: w("<value><i8>%d</i8></value>" % v)
        # Initialize
        self.host = selfIP
        self.port = port
        self.functions = {}
        self.server = None
        if not quiet: print "Function server at {} on port {}.".format(self.host, self.port)

    def assignServer(self, server):
        '''Adds a reference to the server object for two-way communication'''

    def dispatcher(self, functionName, args = None):
        '''Given functionName as a string, return the corresponding function.
        Allows for dynamic function registration.'''
        print self.functions
        if args == None:
            return self.functions[functionName]()
        else:
            return self.functions[functionName](*args)

    def receiveFunctions(self, fnCodeDict):
        #pass
        print fnCodeDict
        for fnName in fnCodeDict:
            fnCode = fnCodeDict[fnName]
            self.functions[fnName] = types.FunctionType(marshal.loads(fnCode.data), globals())
        #self.functions = {types.FunctionType(marshal.loads(fnCode.data), globals()) for fnCode in fnCodeDict}
        print self.functions



if __name__ == '__main__':
    # Node ID retrival
    nodeLocation = str(masterNodeIP) + ":20000"
    nodeServer = xmlrpclib.ServerProxy(nodeLocation)
    while True:
        try:
            nodeID = nodeServer.addNode('http://' + str(selfIP))
            xmlrpcPort = 19000 + int(nodeID)
            # Start function server in separate thread
            functionServerThread = Thread(target = runFunctionServer, args = (xmlrpcPort, False,))
            functionServerThread.daemon = True
            functionServerThread.start()
            nodeServer.nodeReady(str(nodeID))
            break
        except socket.error:
            time.sleep(1)
    # commsPort = 17000 + nodeID

    # commsServer.serve_forever()

    # clientServerThread = Thread(target = startClient)
    # clientServerThread.daemon = True
    # clientServerThread.start()

    while True:  # Loop forever to keep daemon threads alive but be able to exit with ^C
        time.sleep(2)


        # client = Client(host, port)
        # client.close()
