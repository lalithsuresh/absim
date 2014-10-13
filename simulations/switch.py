import SimPy.Simulation as Simulation
import math
import random
import sys
import experiment
from port import Port
from node import Node

class Switch(Node):
    """A representation of a physical switch that disperses requests and responses
    Note: We're assuming that switches aren't bottle-necked by processing times"""
    def __init__(self, id_, htype,
                 procTime):
        Node.__init__(self, htype)
        self.id = id
        self.procTime = procTime
        self.connectedHosts = {}
    
    def addConnectedHosts(self, n, nHosts):
        if n not in self.connectedHosts:
            self.connectedHosts[n] = list(nHosts)
        else:
            self.connectedHosts[n].extend(nHosts)

    def getConnectedHosts(self):
        return self.connectedHosts
          
    def getIntermediary(self, n):
        for k in self.connectedHosts.keys():
            if n in self.connectedHosts[k]:
                return k
        return False
     
    def enqueueTask(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getServiceTime(self, task):
        serviceTime = task.size/self.bw + self.procTime
        return serviceTime
     
class Executor(Simulation.Process):
    
    def __init__(self, switch, task):
        self.switch = switch
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        queueSizeBefore = len(self.switch.queueResource.waitQ)
        yield Simulation.hold, self
        yield Simulation.request, self, self.server.queueResource
        waitTime = Simulation.now() - start
        serviceTime = self.server.getServiceTime()
        yield Simulation.hold, self, serviceTime
        yield Simulation.release, self, self.server.queueResource

        queueSizeAfter = len(self.server.queueResource.waitQ)
        self.task.sigTaskComplete({"waitTime": waitTime,
                                   "serviceTime": serviceTime,
                                   "queueSizeBefore": queueSizeBefore,
                                   "queueSizeAfter": queueSizeAfter})
