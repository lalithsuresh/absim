import SimPy.Simulation as Simulation
import numpy
import random


class MuUpdater(Simulation.Process):

    def __init__(self, server, serviceTimeBase, intervalParam, rangeParam):
        self.server = server
        self.serviceTimeBase = serviceTimeBase
        self.intervalParam = intervalParam
        self.rangeParam = rangeParam
        Simulation.Process.__init__(self, name='MuUpdater')

    def run(self):
        while(1):
            yield Simulation.hold, self, self.intervalParam
            st = numpy.random.pareto(2, 1) + self.serviceTimeBase
            self.server.serviceTime = min(st[0], 80)
            # self.server.serviceTime = \
            #     random.uniform(self.serviceTimeBase,
            #                    self.serviceTimeBase + self.rangeParam)
