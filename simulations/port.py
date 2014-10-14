'''
Created on Oct 9, 2014

@author: Nagwa
'''

import SimPy.Simulation as Simulation
from misc import DeliverMessageWithDelay
class Port():
    def __init__(self, src, dst, bw):
        self.src = src
        self.dst = dst
        self.bw = bw
        #Used to model link bandwidth simply as multiple queues processing tasks concurrently
        #TODO: Requires more sophisticated modeling
        self.buffer = Simulation.Resource(capacity=1, monitored=True)
      
    def enqueueTask(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getServiceTime(self, task):
        serviceTime = task.size/self.bw
        return serviceTime
    
    def getQueueSize(self):
        return len(self.buffer.waitQ)
     
class Executor(Simulation.Process):
    
    def __init__(self, port, task):
        self.port = port
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        queueSizeBefore = len(self.port.buffer.waitQ)
        yield Simulation.hold, self
        yield Simulation.request, self, self.port.buffer
        waitTime = Simulation.now() - start
        serviceTime = self.port.getServiceTime(self.task)
        yield Simulation.hold, self, serviceTime
        yield Simulation.release, self, self.port.buffer

        queueSizeAfter = len(self.port.buffer.waitQ)
        self.task.sigTaskComplete({"waitTime": waitTime,
                                   "serviceTime": serviceTime,
                                   "queueSizeBefore": queueSizeBefore,
                                   "queueSizeAfter": queueSizeAfter})
        
        #Start running the queue task
        self.port.dst.enqueueTask(self.task)
