'''
Created on Oct 9, 2014

@author: Waleed Reda
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
        #if(self.dst.htype != "client" and self.dst.htype != "server"):
            #Check whether queue size threshold has been reached
            #print 'switch buffer', constants.SWITCH_BUFFER_SIZE
        #print self.src.id, self.dst.id, 'wait queue', len(self.buffer.waitQ)
        #if(len(self.buffer.waitQ)>= constants.SWITCH_BUFFER_SIZE):
        #    #Drop packet
        #    task.sigTaskReceived(True)
        #    print '>>>>PACKET DROPPED!, dst:', task.dst.id, task.id
        #    return
        #print 'This is my current Q size:', self.getQueueSize()
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getTxTime(self, task):
        txTime = task.size/self.bw
        return txTime
    
    def getQueueSize(self):
        return len(self.buffer.waitQ)
     
class Executor(Simulation.Process):
    
    def __init__(self, port, task):
        self.port = port
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        start = Simulation.now()
        if(len(self.port.buffer.waitQ)>= constants.SWITCH_BUFFER_SIZE):
            #Drop packet
            self.task.sigTaskReceived(True)
            #print '>>>>PACKET DROPPED!, dst:', self.task.dst.id, self.task.id
            return
        print self.port.src.id, self.port.dst.id, 'wait queue before:', len(self.port.buffer.waitQ)
        #yield Simulation.hold, self
        yield Simulation.request, self, self.port.buffer
        print self.port.src.id, self.port.dst.id, 'wait queue after:', len(self.port.buffer.waitQ)
        waitingTime = Simulation.now() - start
        tx_delay = self.port.getTxTime(self.task)
        prop_delay = constants.NW_LATENCY_BASE
        delay = tx_delay + prop_delay
        yield Simulation.hold, self, delay
        yield Simulation.release, self, self.port.buffer
        #Tracks link waiting times (which is directly correlated if not synonymous with congestion)
        self.port.latencyTrackerMonitor\
            .observe("%s %s" % (self.port.dst.id,
             waitingTime))
                             
        #Forward to the next device
        if(self.port.dst.htype == "client"):
            self.port.dst.receiveResponse(self.task)
        else:
            self.port.dst.enqueueTask(self.task)
