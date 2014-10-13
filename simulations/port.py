'''
Created on Oct 9, 2014

@author: Nagwa
'''

import SimPy.Simulation as Simulation

class Port():
    def __init__(self, src, dst, bw):
        self.src = src
        self.dst = dst
        self.bw = bw
        #Used to model link bandwidth simply as multiple queues processing tasks concurrently
        #TODO: Requires more sophisticated modeling
        self.buffer = Simulation.Resource(capacity=1, monitored=True)
      
        