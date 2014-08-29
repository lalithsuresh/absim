import SimPy.Simulation as Simulation
import random


class MuUpdater(Simulation.Process):

    def __init__(self, server, intervalParam, serviceTime, rateChangeFactor):
        self.server = server
        self.intervalParam = intervalParam
        self.serviceTime = serviceTime
        self.rateChangeFactor = rateChangeFactor
        Simulation.Process.__init__(self, name='MuUpdater')

    def run(self):
        while(1):
            yield Simulation.hold, self, self.intervalParam

            # if (random.uniform(0, 1.0) >= 0.5):
            #     rate = 1/float(self.serviceTime)
            #     self.server.serviceTime = 1/float(rate)
            # else:
            #     rate = 1/float(self.serviceTime)
            #     rate += self.rateChangeFactor * rate
            #     self.server.serviceTime = 1/float(rate)
