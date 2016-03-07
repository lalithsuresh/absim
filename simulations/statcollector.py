'''
Created on 22 Oct 2014

@author: reda
'''

import SimPy.Simulation as Simulation
import constants
import logger

class StatCollector(Simulation.Process):
    '''
    Used for periodically collecting parameters from clients and servers
    '''

    def __init__(self, clients, servers, switches, workloads, numRequests):
        self.clients = clients
        self.servers = servers
        self.switches = switches
        self.workloads = workloads
        self.numRequests = numRequests
        self.reqResDiff = Simulation.Monitor(name="ReqResDiffMonitor")
        self.bwMonitor = Simulation.Monitor(name="BandwidthMonitor")
        self.log = logger.getLogger("StatCollector", constants.LOG_LEVEL)
        Simulation.Process.__init__(self, name='StatCollector')
        
    def run(self, delay):
        percCompletion = 10
        while True:
            totalReqSent = 0
            totalResRecv = 0
            for w in self.workloads:
                totalReqSent += w.taskCounter
            for c in self.clients:
                totalResRecv += c.responsesReceived
            for s in self.switches:
                for n in s.neighbors:
                    p = s.neighbors[n]
                    util = min((p.numPackets * constants.PACKET_SIZE)/(p.bw/1000.0*delay) * 100, 100)
                    p.numPackets = 0
                    self.bwMonitor.observe(util)
            self.reqResDiff.observe(totalReqSent-totalResRecv)
            finishedReqs = all(w.numRequests == 0 for w in self.workloads)
            if((float(totalResRecv)/self.numRequests*100.0) >= percCompletion):
                percCompletion += 10
                self.log.info('Simulation run %d percent complete!'%(float(totalResRecv)/self.numRequests*100.0))
            #This means that all requests have been sent and all responses have been returned
            if((totalReqSent-totalResRecv) == 0 and finishedReqs and not constants.TEST_RUN):
                #set this value to TRUE to terminate any running processes
                constants.END_SIMULATION = True
                return
            yield Simulation.hold, self, delay
