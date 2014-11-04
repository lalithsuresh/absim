'''
Created on Oct 9, 2014

@author: Nagwa
'''
from port import Port
class Node():
    '''
    Generic class that defines switches and end-hosts
    '''
    htype_values = {"client":0, "server":1, "edge":2, "aggr":3, "core":4}
    def __init__(self, id_, htype):
        self.id = id_
        self.neighbors = {}
        self.htype = htype
      
    def addNeighbor(self, n, bw):
        p = Port(self, n, bw)
        self.neighbors[n] = p
      
    def isNeighbor(self, n):
        if n in self.neighbors:
            return True
        else:
            return False
       
    def getNeighbors(self):
        return self.neighbors
    
    def getPort(self, n):
        return self.neighbors[n]
    
    def getUppers(self):
        uppers = []
        for n in self.neighbors:
            if(self.htype_values[self.htype] < self.htype_values[n.htype]):
                uppers.append(n)
        return uppers

    def getDowners(self):
        downers = []
        for n in self.neighbors:
            if(self.htype_values[self.htype] > self.htype_values[n.htype]):
                downers.append(n)
        return downers   
     
    def isHost(self):
        if(self.htype == 'client' or self.htype == 'server'):
            return True
        else:
            return False
        
    def isClient(self):
        if(self.htype == 'client'):
            return True
        else:
            return False
        
    def isServer(self):
        if(self.htype == 'server'):
            return True
        else:
            return False 
               
    def isEdge(self):
        if(self.htype == 'edge'):
            return True
        else:
            return False
       
    def isAggr(self):
        if(self.htype == 'aggr'):
            return True
        else:
            return False
       
    def isCore(self):
        if(self.htype == 'core'):
            return True
        else:
            return False