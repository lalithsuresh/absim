import unittest
import server
import client
import task
import SimPy.Simulation as Simulation


class Observer(Simulation.Process):
    def __init__(self, server, client):
        self.server = server
        self.client = client
        self.monitor = Simulation.Monitor(name="Latency")
        Simulation.Process.__init__(self, name='Observer:' + str(server.id))

    def addNtasks(self, cli, N):
        for i in range(N):  # A burst that uses up all tokens
            taskToSchedule = task.Task("Task%s" % i, self.monitor)
            cli.schedule(taskToSchedule)

    def testBackPressureLoopSingleServer(self):
        yield Simulation.hold, self
        rateLimiter = self.client.rateLimiters[self.server]
        bps = self.client.backpressureSchedulers[self.server]
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
        assert rateLimiter.tokens > 4, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 5, "Tokens is %s" % rateLimiter.tokens

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
        assert rateLimiter.tokens > 0, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 1, "Tokens is %s" % rateLimiter.tokens

        #######################################################
        # Now that the number of tokens is close to zero,
        # add another two requests to the queue
        #######################################################
        self.addNtasks(self.client, 2)
        assert len(bps.backlogQueue) == 2
        yield Simulation.hold, self, 0.0001
        assert len(bps.backlogQueue) == 2   # Backpressure should have happened
        assert rateLimiter.tokens > 0, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 1, "Tokens is %s" % rateLimiter.tokens

        #######################################################
        # At this juncture, the scheduler should not dequeue
        # the remaining request any time before timeToWait milliseconds
        #######################################################
        timeToWait = rateLimiter.tryAcquire()
        yield Simulation.hold, self, timeToWait - 0.0001
        assert len(bps.backlogQueue) == 2   # Should still be in backpressure
        assert rateLimiter.tokens > 0, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 1, "Tokens is %s" % rateLimiter.tokens

        yield Simulation.hold, self, 0.0003
        # Done with one round of backpressure, one more request left
        assert len(bps.backlogQueue) == 1
        # Tokens should still not be enough, and tryAcquire should
        # report a waiting time
        assert rateLimiter.tokens > 0, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 1, "Tokens is %s" % rateLimiter.tokens
        timeToWait = rateLimiter.tryAcquire()
        assert timeToWait > 0

        #######################################################
        # Wait to empty out the last request
        #######################################################
        yield Simulation.hold, self, timeToWait - 0.0001
        assert len(bps.backlogQueue) == 1
        assert rateLimiter.tokens > 0, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 1, "Tokens is %s" % rateLimiter.tokens

        yield Simulation.hold, self, 0.0003
        # Aaaaand the last one is out
        assert len(bps.backlogQueue) == 0
        # Tokens should still not be enough, and tryAcquire should
        # report a waiting time
        assert rateLimiter.tokens > 0, "Tokens is %s" % rateLimiter.tokens
        assert rateLimiter.tokens < 1, "Tokens is %s" % rateLimiter.tokens
        timeToWait = rateLimiter.tryAcquire()
        assert timeToWait > 0


class TestServerLoop(unittest.TestCase):

    # First check if a single task is being executed correctly
    # by a server, then check for two tasks being executed
    # one after the other.
    def testBackPressureLoopSingleServer(self):
        Simulation.initialize()
        s1 = server.Server(1,
                           resourceCapacity=1,
                           serviceTime=4,
                           serviceTimeModel="constant")
        c1 = client.Client(id_="Client1",
                           serverList=[s1],
                           replicaSelectionStrategy="expDelay",
                           accessPattern="uniform",
                           replicationFactor=1,
                           backpressure=True,
                           shadowReadRatio=0.0,
                           rateInterval=20,
                           cubicC=0.000004,
                           cubicSmax=10,
                           cubicBeta=0.2,
                           hysterisisFactor=2,
                           demandWeight=1.0)
        observer = Observer(s1, c1)
        Simulation.activate(observer,
                            observer.testBackPressureLoopSingleServer(),
                            at=1.099)
        Simulation.simulate(until=100)


if __name__ == '__main__':
    unittest.main()
