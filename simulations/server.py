import SimPy.Simulation as Simulation
import math
import random
import sys


class Server():
    """A representation of a physical server that holds resources"""
    def __init__(self, id_, resourceCapacity,
                 serviceTime, serviceTimeModel):
        self.id = id_
        self.serviceTime = serviceTime
        self.serviceTimeModel = serviceTimeModel
        self.queueResource = Simulation.Resource(capacity=resourceCapacity,
                                                 monitored=True)

    def enqueueTask(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

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


class Executor(Simulation.Process):

    def __init__(self, server, task):
        self.server = server
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        queueSizeBefore = len(self.server.queueResource.waitQ)
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
                                   "queueSizeAfter": queueSizeAfter})
