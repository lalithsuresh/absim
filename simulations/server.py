import SimPy.Simulation as Simulation
import math
import random
import sys
import misc
from node import Node
class Server(Node):
    """A representation of a physical server that holds resources"""
    def __init__(self, id_, resourceCapacity,
                 serviceTime, serviceTimeModel):
        Node.__init__(self, id_, "server")
        self.id = id_
        self.serviceTime = serviceTime
        self.serviceTimeModel = serviceTimeModel
        self.queueResource = Simulation.Resource(capacity=resourceCapacity,
                                                 monitored=True)
        self.serverRRMonitor = Simulation.Monitor(name="ServerMonitor")

    def enqueueTask(self, task):
        if(task.isCut):
            #This is a notification of a packet drop
            #Resend packet
            nextSwitch = self.getNeighbors().keys()[0]
            # Get port I'm delivering through
            egress = self.getPort(nextSwitch)
            response = misc.cloneDataTask(task)
            response.restorePacket()
            # resend response
            egress.enqueueTask(response)
            return
        
        executor = Executor(self, task)
        self.serverRRMonitor.observe(1)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getServiceTime(self):
        serviceTime = 0.0
        if (self.serviceTimeModel == "random.expovariate"):
            serviceTime = random.expovariate(1.0/(self.serviceTime))
        elif (self.serviceTimeModel == "constant"):
            serviceTime = self.serviceTime
        elif(self.serviceTimeModel == "math.sin"):
            serviceTime = self.serviceTime \
                + self.serviceTime \
                * math.sin(1 + Simulation.now()/100)
        else:
            print "Unknown service time model"
            sys.exit(-1)

        return serviceTime


class Executor(Simulation.Process):

    def __init__(self, server, task):
        self.server = server
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        queueSizeBefore = len(self.server.queueResource.waitQ)
        totalQueueSizeBefore = len(self.server.queueResource.waitQ + self.server.queueResource.activeQ)
        ideallySortedReplicaSet = self.sort(self.task.replicaSet)
        yield Simulation.hold, self
        yield Simulation.request, self, self.server.queueResource
        waitTime = Simulation.now() - start         # W_i
        serviceTime = self.server.getServiceTime()  # Mu_i
        yield Simulation.hold, self, serviceTime
        yield Simulation.release, self, self.server.queueResource

        queueSizeAfter = len(self.server.queueResource.waitQ)
        self.task.sigTaskComplete({"waitingTime": waitTime,
                                   "serviceTime": serviceTime,
                                   "queueSizeBefore": queueSizeBefore,
                                   "queueSizeAfter": queueSizeAfter,
                                   "totalQueueSizeBefore": totalQueueSizeBefore,
                                   "idealReplicaSet": ideallySortedReplicaSet})
        
        for i in xrange(1, self.task.count+1):
            respPacket = misc.cloneDataTask(self.task)
            respPacket.count = self.task.count
            respPacket.seqN = i
            respPacket.dst = self.task.src
            respPacket.src = self.task.dst
            respPacket.setRequest(self.task)
            # Get switch I'm delivering to
            nextSwitch = self.server.getNeighbors().keys()[0]
            # Get port I'm delivering through
            egress = self.server.getPort(nextSwitch)
            # Immediately send out request
            egress.enqueueTask(respPacket)
                  
    #This is just used for reporting selection errors
    def sort(self, replicaSet):
            # Sort by response times * pending-requests
            futureOracleMap = {replica: (1 + len(replica.queueResource.activeQ
                                   + replica.queueResource.waitQ))
                         * replica.serviceTime
                         for replica in replicaSet}
            sortedReplicaSet = sorted(replicaSet, key=futureOracleMap.get)
            return sortedReplicaSet