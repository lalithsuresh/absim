'''
Created on Oct 9, 2014

@author: Nagwa
'''

import SimPy.Simulation as Simulation
import constants
import random

class Port():
    def __init__(self, src, dst, bw):
        self.src = src
        self.dst = dst
        self.bw = bw
        #Used to model link bandwidth simply as multiple queues processing tasks concurrently
        #TODO: Requires more sophisticated modeling
        self.buffer = Simulation.Resource(capacity=1, monitored=True)
        self.latencyTrackerMonitor = Simulation.Monitor(name="LatencyTracker")
      
    def enqueueTask(self, task):
        #Check whether the dst is a switch (since we don't add buffer restrictions to clients/servers)
        if(self.dst.htype != "client" and self.dst.htype != "server"):
            #Check whether queue size threshold has been reached
            if(len(self.buffer.waitQ)>= constants.SWITCH_BUFFER_SIZE):
                #Drop packet
                task.sigTaskReceived(True)
                #print '>>>>PACKET DROPPED!, dst:', task.dst.id
                return
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getTravelTime(self, task):
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
        #Some random delay caused by the stochastic nature of communication signals
        delay = constants.NW_LATENCY_BASE + \
        random.normalvariate(constants.NW_LATENCY_MU,
                            constants.NW_LATENCY_SIGMA)
        yield Simulation.hold, self, delay
        queueSizeBefore = len(self.port.buffer.waitQ)
        yield Simulation.hold, self
        yield Simulation.request, self, self.port.buffer
        waitingTime = Simulation.now() - start
        travelTime = self.port.getTravelTime(self.task)
        yield Simulation.hold, self, travelTime
        yield Simulation.release, self, self.port.buffer
        latency = waitingTime + travelTime
        queueSizeAfter = len(self.port.buffer.waitQ)

        #Tracks link waiting times (which is directly correlated if not synonymous with congestion)
        self.port.latencyTrackerMonitor\
            .observe("%s %s" % (self.port.dst.id,
             waitingTime))
                             
        #Forward to the next device
        if(self.port.dst.htype == "client"):
            self.port.dst.receiveResponse(self.task)
        else:
            self.port.dst.enqueueTask(self.task)
