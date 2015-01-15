'''
Created on 22 Oct 2014

@author: reda
'''

import SimPy.Simulation as Simulation
import constants
import random

class StatCollector(Simulation.Process):
    '''
    Used for periodically collecting parameters from clients and servers
    '''

    def __init__(self, clients, servers, switches, workloads):
        self.clients = clients
        self.servers = servers
        self.switches = switches
        self.workloads = workloads
        self.reqResDiff = Simulation.Monitor(name="ReqResDiffMonitor")
        Simulation.Process.__init__(self, name='StatCollector')
        
    def run(self, delay):
        while True:
            totalReqSent = 0
            totalResRecv = 0
            for w in self.workloads:
                totalReqSent += w.taskCounter
            for c in self.clients:
                totalResRecv += c.responsesReceived
            self.reqResDiff.observe(totalReqSent-totalResRecv)
            finishedReqs = all(w.numRequests == 0 for w in self.workloads)
            #This means that all requests have been sent and all responses have been returned
            if((totalReqSent-totalResRecv) == 0 and finishedReqs):
                #terminate switches, to stop the periodic DRE update function
                for s in self.switches:
                    s.active = False
                return
            yield Simulation.hold, self, delay