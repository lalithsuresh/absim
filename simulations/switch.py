import SimPy.Simulation as Simulation
import math
import random
import sys
import experiment
from port import Port
from node import Node
from misc import DeliverMessageWithDelay
from misc import cloneDataTask
import constants

class Switch(Node):
    """A representation of a physical switch that disperses requests and responses
    Note: We're assuming that switches aren't bottle-necked by processing times"""
    def __init__(self, id_, htype,
                 procTime):
        Node.__init__(self, id_, htype)
        self.procTime = procTime
        self.connectedHosts = {} #Equivalent to switch routing table
        self.queueResource = Simulation.Resource(capacity=1, monitored=True)
    
    def addConnectedHosts(self, n, nHosts):
        for h in nHosts:         
            if h not in self.connectedHosts:
                self.connectedHosts[h] = [n]
            else:
                self.connectedHosts[h].append(n)

    def getConnectedHosts(self):
        return self.connectedHosts.keys()
          
    def getIntermediary(self, n):
        for k in self.connectedHosts.keys():
            if n in self.connectedHosts[k]:
                return k
        return False
     
    def enqueueTask(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getServiceTime(self):
        #serviceTime = task.size/self.bw + self.procTime
        return self.procTime
        #return serviceTime
        
        #Dynamic Load-Balaning algorithm (DLB) from OpenFlow based Load Balancing for Fat-Tree Networks with Multipath Support
    def getNextHop(self, dst):
        #check if I'm direct neighbors with dst
        if(self.isNeighbor(dst)):
            egressPort = self.getPort(dst)
        #check if I'm connected to dst through intermediary node
        elif(self.getIntermediary(dst)):
            possible_hops = self.getIntermediary(dst)
            egressPort = self.getPortWithLowestQueue(possible_hops)
        else: 
            #We're going up!
            possible_hops = self.getUppers()
            egressPort = self.getPortWithLowestQueue(possible_hops)
        return egressPort
     
    def getPortWithLowestQueue(self, nodes):
        queue_size = 99999999
        egress = None
        for n in nodes:
            port = self.getPort(n)
            if(port.getQueueSize() < queue_size):
                queue_size = port.getQueueSize()
                egress = self.getPort(n)
        return egress
    
class Executor(Simulation.Process):
    
    def __init__(self, switch, task):
        self.switch = switch
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        queueSizeBefore = len(self.switch.queueResource.waitQ)
        yield Simulation.hold, self
        yield Simulation.request, self, self.switch.queueResource
        waitTime = Simulation.now() - start
        serviceTime = self.switch.getServiceTime()
        yield Simulation.hold, self, serviceTime
        yield Simulation.release, self, self.switch.queueResource

        queueSizeAfter = len(self.switch.queueResource.waitQ)
        self.task.sigTaskComplete({"waitTime": waitTime,
                                   "serviceTime": serviceTime,
                                   "queueSizeBefore": queueSizeBefore,
                                   "queueSizeAfter": queueSizeAfter})
        
        #Make the next hop
        newTask = cloneDataTask(self.task)
        delay = constants.NW_LATENCY_BASE + \
        random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)
        #print newTask.dst
        #print self.switch.neighbors
        egress = self.switch.getNextHop(newTask.dst)
        for h in self.switch.getConnectedHosts():
            print 'hosts:', h.id
        print 'switch', self.switch.id, 'sending to:', egress.dst.id
        #print 'test2', egress
        messageDeliveryProcess = DeliverMessageWithDelay()
        Simulation.activate(messageDeliveryProcess,
                            messageDeliveryProcess.run(newTask,
                                                       delay,
                                                       egress),
                            at=Simulation.now())