'''
Created on Oct 9, 2014

@author: Waleed Reda
'''

import SimPy.Simulation as Simulation
import constants
import random
import misc

class Port():
    def __init__(self, src, dst, bw):
        self.src = src
        self.dst = dst
        self.bw = bw
        #Used to model link bandwidth simply as multiple queues processing tasks concurrently
        #TODO: Requires more sophisticated modeling
        self.buffer = Simulation.Resource(capacity=1, monitored=True)
        self.latencyTrackerMonitor = Simulation.Monitor(name="LatencyTracker")
        self.numCutPackets = 0
      
    def enqueueTask(self, task):
        #Check whether the dst is a switch (since we don't add buffer restrictions to clients/servers)
        #if(self.dst.htype != "client" and self.dst.htype != "server"):
            #Check whether queue size threshold has been reached
            #print 'switch buffer', constants.SWITCH_BUFFER_SIZE
        #print self.src.id, self.dst.id, 'wait queue', len(self.buffer.waitQ)
        #if(task.isCut):
        #    self.numCutPackets += 1  
        #elif((len(self.buffer.waitQ) - self.numCutPackets) >= constants.SWITCH_BUFFER_SIZE):
            #print '>>>>PACKET DROPPED!, dst:', task.dst.id, task.id
        #    return False
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
        if(self.task.isCut):
            self.port.numCutPackets += 1  
        elif((len(self.port.buffer.waitQ) - self.port.numCutPackets) >= constants.SWITCH_BUFFER_SIZE and not self.port.src.htype == "client" and not self.port.src.htype == "server"):
            #print 'buffer queue size', len(self.port.buffer.waitQ), 'max buffer size', constants.SWITCH_BUFFER_SIZE
            #Packet was dropped; Forward packet header to the reverse path
            dropNotif = misc.cloneDataTask(self.task)
            dropNotif.cutPacket()
            #print 'Packet dropped. Sending header to reverse path. Dst:%s, Type:%s'%(dropNotif.dst.id, dropNotif.dst.htype)
            #print 'src:%s, dst:%s'%(self.task.src, self.task.dst)
            self.port.src.enqueueTask(dropNotif)
            return
        #print self.port.src.id, self.port.dst.id, 'wait queue before:', len(self.port.buffer.waitQ)
        #yield Simulation.hold, self
        yield Simulation.request, self, self.port.buffer
        #print self.port.src.id, self.port.dst.id, 'wait queue after:', len(self.port.buffer.waitQ)
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
        
        if(self.task.isCut):
            self.port.numCutPackets -= 1                  
        #Forward to the next device
        if(self.port.dst.htype == "client"):
            self.port.dst.receiveResponse(self.task)
        else:
            self.port.dst.enqueueTask(self.task)
