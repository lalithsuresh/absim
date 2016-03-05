import SimPy.Simulation as Simulation
import constants
import misc
import random
import numpy as np
import scipy.interpolate as interpolate
from misc import BackgroundTrafficGenerator

class Spawner():
    def __init__(self):

        self.hostList = constants.TOPOLOGY.HostList

        #Flow size CDF (numPackets, 1, CDF)
        '''
        6 1 0
        6 1 0.15
        13 1 0.2
        19 1 0.3
        33 1 0.4
        53 1 0.53
        133 1 0.6
        667 1 0.7
        1333 1 0.8
        3333 1 0.9
        6667 1 0.97
        20000 1 1
        '''

        bins = np.array([0, 0.15, 0.2, 0.3, 0.4, 0.53, 0.6, 0.7, 0.8, 0.9, 0.97, 1])
        data = np.array([6, 6, 13, 19, 33, 53, 133, 667, 1333, 3333, 6667, 20000])
        self.cdf = interpolate.interp1d(self.data, bins)

        self.flowDistr = np.cumsum()


    def run(self):
        while(not constants.END_SIMULATION):
            targetHost = random.choice(self.hostList)
            self.spawnFlow(self.spawner.getFlowSizeSample())
            yield Simulation.hold, self, self.delay

    def getFlowSizeSample(self):
        r = np.random.rand()
        flowsize = int(self.cdf(r))
        assert(flowsize > 0)
        return flowsize

    def getHosts(self):
        src = random.choice(self.hostList)
        dst = src
        while (src != dst):
            dst = random.choice(self.hostList)
        return (src, dst)

    def spawnFlow(self):
        #pick src and dest at random
        #FIXME should we introduce bias?

        hosts = self.getHosts()
        executor = BackgroundTrafficGenerator(self, hosts[0], hosts[1], self.getFlowSizeSample())
        Simulation.activate(executor, executor.run(), Simulation.now())
