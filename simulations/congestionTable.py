'''
Created on 24 Nov 2014

@author: reda
'''
import sys
from collections import defaultdict
from math import ceil

class CongestionTable():
    def __init__(self):
        self.congestionFrom = defaultdict(dict)
        self.valueChanged = defaultdict(dict)
        self.congestionTo = defaultdict(dict)
        
    def updateFrom(self, switch, port, metric):
        if(self.congestionFrom.get(switch) is None):
            self.congestionFrom[switch][port] = metric
            self.valueChanged[switch][port] = True  
        elif(not self.congestionFrom.get(switch).get(port) == metric):
            self.congestionFrom[switch][port] = metric
            self.valueChanged[switch][port] = True
        
    def updateTo(self, switch, port, metric):
        self.congestionTo[switch][port] = metric

    def getFrom(self, switch):
        if switch in self.congestionFrom.keys():
            #Get the first port who's value has changed
            for p in self.congestionFrom[switch].keys():
                if(self.valueChanged[switch][p]):
                    self.valueChanged[switch][p] = True
                    return p, self.congestionFrom[switch][p]
            return self.congestionFrom[switch].keys().get(0), self.congestionFrom[switch][self.congestionFrom[switch].keys().get(0)]
        else:
            return False, False
    
    def getTo(self, switch, possible_ports=False):
        ceMin = sys.maxint
        pMin = None
        if(possible_ports):
            ports = possible_ports
        else:
            ports = switch.neighbors.values()
        for p in ports:
            ceTable  = self.congestionTo[switch].get(p, 0)
            ce = max(ceTable, p.ce)
            if(ce<ceMin):
                ceMin = ce
                pMin = p
        return pMin, ceMin
    
    def getToCE(self, switch, possible_ports=False):
        ceMin = sys.maxint
        if(possible_ports):
            ports = possible_ports
        else:
            ports = switch.neighbors.values()
        for p in ports:
            ceTable  = self.congestionTo[switch].get(p, 0)
            ce = max(ceTable, p.ce)
            if(ce<ceMin):
                ceMin = ce
        print self.congestionTo[switch]
        return ceMin