'''
Created on Oct 9, 2014

@author: Waleed Reda
'''

import SimPy.Simulation as Simulation
import constants
import random
import misc
import logger

class Port():
    def __init__(self, src, dst, bw):
        self.src = src
        self.dst = dst
        self.bw = bw
        self.ce = 0.0
        self.pckt_acc = 0.0
        #Used to model link bandwidth simply as multiple queues processing tasks concurrently
        #TODO: Requires more sophisticated modeling
        self.buffer = Simulation.Resource(capacity=1, monitored=True)
        self.latencyTrackerMonitor = Simulation.Monitor(name="LatencyTracker")
        self.numCutPackets = 0
        self.numPackets = 0
        self.numDropNotifs = 0
        self.log = logger.getLogger(str(self.src) + ":" + str(self.dst), constants.LOG_LEVEL)
      
    def enqueueTask(self, task):
        #print 'Enqueueing request:%s to port [src:%s, dst:%s]'%(task.id, self.src.id, self.dst.id)
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
        #self.log.debug("PORT ENQUEUE %s %s %s %s %s"%(task.id, task.src, task.dst, task.isCut,task.trafficType))
        self.pckt_acc += task.size * 1000 #MB --> B
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getTxTime(self, task):
        txTime = task.size/float(self.bw)*1000000
        return txTime

    def getTxTime_size(self, size):
        txTime = size/float(self.bw)*1000000
        return txTime
      
    def getQueueSize(self):
        return len(self.buffer.waitQ)
    
    def updateDRE(self):
        #print self.dre
        self.pckt_acc = self.pckt_acc/constants.CE_UPDATE_PERIOD #bytes per millisecond
        #FIXME check if below formula is correct
        self.pckt_acc = self.pckt_acc/(self.bw*1000) #
        self.ce = self.pckt_acc * constants.CE_WEIGHT + self.ce * (1-constants.CE_WEIGHT)
        self.pckt_acc = 0
 
    def isFull(self):
        if((len(self.buffer.waitQ) - self.numCutPackets) >= constants.SWITCH_BUFFER_SIZE and not self.src.htype == "client" and not self.src.htype == "server"):
            return True
        else:
            return False

    def __str__(self):
        return "Src:%s Dst:%s"%(self.src.id, self.dst.id)
    
    def __repr__(self):
        return self.__str__()
         
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
            self.port.numDropNotifs += 1
            dropNotif = misc.cloneDataTask(self.task)
            dropNotif.setServerFB(self.task.serverFB)
            dropNotif.requestTask = self.task.requestTask
            dropNotif.copyCONGAParams(self.task)
            dropNotif.cutPacket()
            #self.port.log.debug("Cutting packet %s %s %s"%(dropNotif.id, dropNotif.src, dropNotif.dst))
            #print self.task.seqN, 'Packet dropped. Sending header to reverse path. Dst:%s, Type:%s'%(dropNotif.dst.id, dropNotif.dst.htype)
            #print 'src:%s, dst:%s'%(self.task.src, self.task.dst)
            self.port.src.enqueueTask(dropNotif)
            return
        else:
            #add packet to the counter (used for measuring bandwidth util)
            self.port.numPackets += 1

        #print self.port.src.id, self.port.dst.id, 'wait queue before:', len(self.port.buffer.waitQ)
        #yield Simulation.hold, self
        #print len(self.port.buffer.waitQ)
        #entryTime = Simulation.now()
        #queuesize = len(self.port.buffer.waitQ)

        yield Simulation.request, self, self.port.buffer
        #print self.port.src.id, self.port.dst.id, 'wait queue after:', len(self.port.buffer.waitQ)

        waitingTime = Simulation.now() - start
        tx_delay = self.port.getTxTime(self.task)

        yield Simulation.hold, self, tx_delay
        yield Simulation.release, self, self.port.buffer

        prop_delay = constants.PROPAGATION_DELAY
        yield Simulation.hold, self, prop_delay

        #print 'Time spent', Simulation.now() - entryTime, self.port.src, self.port.dst, queuesize
        #Tracks link waiting times (which is directly correlated if not synonymous with congestion)

        self.port.latencyTrackerMonitor\
            .observe("%s %s" % (self.port.dst.id,
             waitingTime))
        
        if(self.task.isCut):
            self.port.numCutPackets -= 1

        #Forward to the next device
        if(self.port.dst.htype == "client"):
            #print 'PORT INFO:', self.port.src.id, self.port.dst.id
            self.port.dst.receiveResponse(self.task)
        else:
            self.port.dst.enqueueTask(self.task)
