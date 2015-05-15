import unittest
import server
import client
import task
import SimPy.Simulation as Simulation


class Observer(Simulation.Process):
    def __init__(self, serverList, client):
        self.serverList = serverList
        self.client = client
        self.monitor = Simulation.Monitor(name="Latency")
        self.taskCounter = 0
        Simulation.Process.__init__(self, name='Observer')

    def addNtasks(self, cli, N):
        for i in range(N):  # A burst that uses up all tokens
            taskToSchedule = task.Task("Task%s%s" % (i, self.taskCounter),
                                       "Task%s%s" % (i, self.taskCounter), 1,
                                       self.monitor, self.client)
            cli.schedule(taskToSchedule, self.serverList)
        self.taskCounter += 1

    def testBackPressureLoopSingleServer1(self):
        yield Simulation.hold, self
        rateLimiter = self.client.rateLimiters[self.serverList[0]]
        bps = self.client.backpressureSchedulers[self.serverList[0]]
        assert rateLimiter is not None

        #######################################################
        # Single task insertion, single rate limiter
        #######################################################
        rateLimiter.rate = 5
        rateLimiter.tokens = 5
        self.addNtasks(self.client, 1)
        assert len(bps.backlogQueue) == 1
        yield Simulation.hold, self, 0.0001
        assert len(bps.backlogQueue) == 0
        assert rateLimiter.getTokens() > 4
        assert rateLimiter.getTokens() < 5

    def testBackPressureLoopSingleServer2(self):
        yield Simulation.hold, self
        rateLimiter = self.client.rateLimiters[self.serverList[0]]
        bps = self.client.backpressureSchedulers[self.serverList[0]]
        assert rateLimiter is not None

        #######################################################
        # Multiple task insertion, single rate limiter
        #######################################################
        rateLimiter.rate = 5
        rateLimiter.tokens = 5

        # A burst that uses up all tokens
        self.addNtasks(self.client, 5)

        assert len(bps.backlogQueue) == 5
        yield Simulation.hold, self, 0.0001
        assert len(bps.backlogQueue) == 0
        assert rateLimiter.getTokens() > 0
        assert rateLimiter.getTokens() < 1

    def testBackPressureLoopSingleServer3(self):
        yield Simulation.hold, self
        rateLimiter = self.client.rateLimiters[self.serverList[0]]
        bps = self.client.backpressureSchedulers[self.serverList[0]]
        assert rateLimiter is not None

        rateLimiter.rate = 5
        rateLimiter.tokens = 0.0

        #######################################################
        # Now that the number of tokens is close to zero,
        # add two requests to the queue
        #######################################################
        self.addNtasks(self.client, 2)
        assert len(bps.backlogQueue) == 2
        yield Simulation.hold, self, 0.0001
        assert len(bps.backlogQueue) == 2   # Backpressure should have happened
        assert rateLimiter.getTokens() > 0
        assert rateLimiter.getTokens() < 1

        #######################################################
        # At this juncture, the scheduler should not dequeue
        # the remaining request any time before timeToWait milliseconds
        #######################################################
        timeToWait = rateLimiter.tryAcquire()
        assert timeToWait > 0
        yield Simulation.hold, self, timeToWait - 1.0001
        assert len(bps.backlogQueue) == 2   # Should still be in backpressure
        assert rateLimiter.getTokens() > 0
        assert rateLimiter.getTokens() < 1

        yield Simulation.hold, self, 1.0002
        # Done with one round of backpressure, one more request left
        assert len(bps.backlogQueue) == 1
        # Tokens should still not be enough, and tryAcquire should
        # report a waiting time
        assert rateLimiter.getTokens() > 0
        assert rateLimiter.getTokens() < 1
        timeToWait = rateLimiter.tryAcquire()
        assert timeToWait > 0

        #######################################################
        # Wait to empty out the last request
        #######################################################
        yield Simulation.hold, self, timeToWait - 1.0005
        assert len(bps.backlogQueue) == 1, len(bps.backlogQueue)
        assert rateLimiter.getTokens() > 0
        assert rateLimiter.getTokens() < 1

        yield Simulation.hold, self, 1.0006
        # Aaaaand the last one is out
        assert len(bps.backlogQueue) == 0
        # Tokens should still not be enough, and tryAcquire should
        # report a waiting time
        assert rateLimiter.getTokens() > 0
        assert rateLimiter.getTokens() < 1
        timeToWait = rateLimiter.tryAcquire()
        assert timeToWait > 0

    def testBackPressureLoopTwoServers(self):
        yield Simulation.hold, self
        rateLimiter1 = self.client.rateLimiters[self.serverList[0]]
        rateLimiter2 = self.client.rateLimiters[self.serverList[1]]
        bps = self.client.backpressureSchedulers[self.serverList[0]]
        assert rateLimiter1 is not None
        assert rateLimiter2 is not None

        #######################################################
        # Exhaust first rate limiter, then the second.
        # We can do this because the replica selection
        # scheme here is 'primary'
        #######################################################
        rateLimiter1.rate = 5
        rateLimiter1.tokens = 2
        rateLimiter1.maxTokens = 2
        rateLimiter2.rate = 10
        rateLimiter2.tokens = 2
        rateLimiter2.maxTokens = 2
        self.addNtasks(self.client, 1)
        assert len(bps.backlogQueue) == 1

        yield Simulation.hold, self, 0.000001
        assert len(bps.backlogQueue) == 0
        assert rateLimiter1.getTokens() >= 1 and rateLimiter1.getTokens() < 2
        assert rateLimiter2.tokens == 2

        self.addNtasks(self.client, 1)
        yield Simulation.hold, self, 0.000001
        assert len(bps.backlogQueue) == 0
        assert rateLimiter1.getTokens() >= 0 and rateLimiter1.getTokens() < 1
        assert rateLimiter2.tokens == 2

        self.addNtasks(self.client, 1)
        yield Simulation.hold, self, 0.000001
        assert len(bps.backlogQueue) == 0
        assert rateLimiter1.getTokens() >= 0 and rateLimiter1.getTokens() < 1
        assert rateLimiter2.getTokens() >= 1 and rateLimiter2.getTokens() < 2

        self.addNtasks(self.client, 1)
        yield Simulation.hold, self, 0.000001
        assert len(bps.backlogQueue) == 0
        assert rateLimiter1.getTokens() >= 0 and rateLimiter1.getTokens() < 1
        assert rateLimiter2.getTokens() >= 0 and rateLimiter2.getTokens() < 1

        #######################################################
        # And now, backpressure
        #######################################################

        self.addNtasks(self.client, 2)
        yield Simulation.hold, self, 0.000001
        assert len(bps.backlogQueue) == 2
        assert rateLimiter1.getTokens() >= 0 and rateLimiter1.getTokens() < 1
        assert rateLimiter2.getTokens() >= 0 and rateLimiter2.getTokens() < 1

        timeToWait2 = rateLimiter2.tryAcquire()
        # Both rate limiters got exhausted very close in time,
        # but rateLimiter2's rate is higher, meaning that the
        # system should leave backpressure after timeToWait2 ms.
        yield Simulation.hold, self, timeToWait2 - 1.001
        assert len(bps.backlogQueue) == 2
        assert rateLimiter1.getTokens() >= 0 and rateLimiter1.getTokens() < 1
        assert rateLimiter2.getTokens() >= 0 and rateLimiter2.getTokens() < 1

        yield Simulation.hold, self, 1.002
        assert len(bps.backlogQueue) == 1, len(bps.backlogQueue)
        assert rateLimiter1.getTokens() >= 0 and rateLimiter1.getTokens() < 1
        assert rateLimiter2.getTokens() >= 0 and rateLimiter2.getTokens() < 1


class TestServerLoop(unittest.TestCase):

    # First check if a single task is being executed correctly
    # by a server, then check for two tasks being executed
    # one after the other.
    def testBackPressureLoopSingleServer1(self):
        Simulation.initialize()
        s1 = server.Server(1,
                           resourceCapacity=1,
                           serviceTime=4,
                           serviceTimeModel="constant")
        c1 = client.Client(id_="Client1",
                           serverList=[s1],
                           replicaSelectionStrategy="C3",
                           accessPattern="uniform",
                           replicationFactor=1,
                           backpressure=True,
                           shadowReadRatio=0.0,
                           rateInterval=20,
                           cubicC=0.000004,
                           cubicSmax=10,
                           cubicBeta=0.2,
                           hysterisisFactor=2,
                           demandWeight=1.0,
                           costExponent=3,
                           concurrencyWeight=1)
        observer = Observer([s1], c1)
        Simulation.activate(observer,
                            observer.testBackPressureLoopSingleServer1(),
                            at=0.1)
        Simulation.simulate(until=100)

    def testBackPressureLoopSingleServer2(self):
        Simulation.initialize()
        s1 = server.Server(1,
                           resourceCapacity=1,
                           serviceTime=4,
                           serviceTimeModel="constant")
        c1 = client.Client(id_="Client1",
                           serverList=[s1],
                           replicaSelectionStrategy="C3",
                           accessPattern="uniform",
                           replicationFactor=1,
                           backpressure=True,
                           shadowReadRatio=0.0,
                           rateInterval=20,
                           cubicC=0.000004,
                           cubicSmax=10,
                           cubicBeta=0.2,
                           hysterisisFactor=2,
                           demandWeight=1.0,
                           costExponent=3,
                           concurrencyWeight=1)
        observer = Observer([s1], c1)
        Simulation.activate(observer,
                            observer.testBackPressureLoopSingleServer2(),
                            at=0.1)
        Simulation.simulate(until=100)

    def testBackPressureLoopSingleServer3(self):
        Simulation.initialize()
        s1 = server.Server(1,
                           resourceCapacity=1,
                           serviceTime=4,
                           serviceTimeModel="constant")
        c1 = client.Client(id_="Client1",
                           serverList=[s1],
                           replicaSelectionStrategy="C3",
                           accessPattern="uniform",
                           replicationFactor=1,
                           backpressure=True,
                           shadowReadRatio=0.0,
                           rateInterval=20,
                           cubicC=0.000004,
                           cubicSmax=10,
                           cubicBeta=0.2,
                           hysterisisFactor=2,
                           demandWeight=1.0,
                           costExponent=3,
                           concurrencyWeight=1)
        observer = Observer([s1], c1)
        Simulation.activate(observer,
                            observer.testBackPressureLoopSingleServer3(),
                            at=0.1)
        Simulation.simulate(until=100)

    def testBackPressureLoopTwoServers(self):
        Simulation.initialize()
        s1 = server.Server(1,
                           resourceCapacity=1,
                           serviceTime=4,
                           serviceTimeModel="constant")
        s2 = server.Server(2,
                           resourceCapacity=1,
                           serviceTime=4,
                           serviceTimeModel="constant")
        c1 = client.Client(id_="Client1",
                           serverList=[s1, s2],
                           replicaSelectionStrategy="PRIMARY",
                           accessPattern="uniform",
                           replicationFactor=2,
                           backpressure=True,
                           shadowReadRatio=0.0,
                           rateInterval=20,
                           cubicC=0.000004,
                           cubicSmax=10,
                           cubicBeta=0.2,
                           hysterisisFactor=2,
                           demandWeight=1.0,
                           costExponent=3,
                           concurrencyWeight=1)
        observer = Observer([s1, s2], c1)
        Simulation.activate(observer,
                            observer.testBackPressureLoopTwoServers(),
                            at=0.1)
        Simulation.simulate(until=100)


if __name__ == '__main__':
    unittest.main()
