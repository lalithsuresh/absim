import SimPy.Simulation as Simulation
import math
import random
import sys
import misc
from node import Node
from scipy.stats import genpareto

class Server(Node):
    """A representation of a physical server that holds resources"""
    def __init__(self, id_, resourceCapacity,
                 serviceTime, serviceTimeModel, valueSizeModel):
        Node.__init__(self, id_, "server")
        self.serviceTime = serviceTime
        self.serviceTimeModel = serviceTimeModel
        self.queueResource = Simulation.Resource(capacity=resourceCapacity,
                                                 monitored=True)
        self.valueSizeModel = valueSizeModel
        self.sentResponses = []

    def enqueueTask(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())
        task.sigTaskReceived(False)

    def getServiceTime(self):
        serviceTime = 0.0
        if (self.serviceTimeModel == "random.expovariate"):
            serviceTime = random.expovariate(1.0/(self.serviceTime))
        elif (self.serviceTimeModel == "constant"):
            serviceTime = self.serviceTime
        elif(self.serviceTimeModel == "math.sin"):
            serviceTime = self.serviceTime \
                + self.serviceTimeParams[0] \
                * math.sin(self.serviceTimeParams[2]
                + Simulation.now()/float(self.serviceTimeParams[1]))
        else:
            print "Unknown service time model"
            sys.exit(-1)

        return serviceTime

    def getResponsePacketCount(self):
        if(self.valueSizeModel == "pareto"):
            numargs = genpareto.numargs
            [ c ] = [0.15,]*numargs
            r = genpareto.rvs(c, loc=0, scale=16.02)
            print int(r)
            return int(r)
        else:
            return 15

class Executor(Simulation.Process):

    def __init__(self, server, task):
        self.server = server
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        queueSizeBefore = len(self.server.queueResource.waitQ)
        #yield Simulation.hold, self
        yield Simulation.request, self, self.server.queueResource
        waitTime = Simulation.now() - start         # W_i
        serviceTime = self.server.getServiceTime()  # Mu_i
        yield Simulation.hold, self, serviceTime
        yield Simulation.release, self, self.server.queueResource

        queueSizeAfter = len(self.server.queueResource.waitQ)
        self.task.sigTaskComplete({"waitTime": waitTime,
                                   "serviceTime": serviceTime,
                                   "queueSizeBefore": queueSizeBefore,
                                   "queueSizeAfter": queueSizeAfter})
        
        packet_count = self.server.getResponsePacketCount()
        copyReceivedEvent = False #For the server, we want to create new events (since the client shouldn't care about response drops)
        for i in xrange(1, packet_count+1):
            respPacket = misc.cloneDataTask(self.task, copyReceivedEvent)
            respPacket.count = packet_count
            respPacket.seqN = i
            respPacket.dst = self.task.src
            respPacket.src = self.task.dst
            # Get switch I'm delivering to
            nextSwitch = self.server.getNeighbors().keys()[0]
            # Get port I'm delivering through
            egress = self.server.getPort(nextSwitch)
            # Immediately send out request
            egress.enqueueTask(respPacket)
            self.server.sentResponses.append(respPacket)
            receiptTracker = ReceiptTracker()
            Simulation.activate(receiptTracker,
                            receiptTracker.run(self.server, respPacket),
                            at=Simulation.now())
            
class ReceiptTracker(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name="ReceiptTracker")
        
    def run(self, server, task):
        yield Simulation.hold, self,
        yield Simulation.waitevent, self, task.receivedEvent
        server.sentResponses.remove(task)
        
        if(task.receivedEvent.signalparam): #This means that the packet has been dropped
            #print 'Server is resending response with ID:', task.id
            nextSwitch = server.getNeighbors().keys()[0]
            # Get port I'm delivering through
            egress = server.getPort(nextSwitch)
            #print 'test1', egress
            # resend request after a small delay
            task.receivedEvent = Simulation.SimEvent("PacketReceipt")
            yield Simulation.hold, self, 0.1
            egress.enqueueTask(task)
            server.sentResponses.append(task)
            receiptTracker = ReceiptTracker()
            Simulation.activate(receiptTracker,
                            receiptTracker.run(server, task),
                            at=Simulation.now())
