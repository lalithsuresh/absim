import unittest
import client
import SimPy.Simulation as Simulation


class Observer(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='Observer')

    def consumeNTokens(self, rl, N):
        for i in range(N):
            if (rl.tryAcquire() == 0):
                rl.update()

    def forceConsumeNTokens(self, rl, N):
        for i in range(N):
            rl.update()

    def test1(self):
        yield Simulation.hold, self,
        rl = client.RateLimiter("RL-1", self, 10, 20)
        rl.rate = 5   # Five tokens per 20ms
        rl.tokens = 10
        assert rl.getTokens() == 10

        # Waiting longer should not increase the number
        # of tokens beyond the maximum
        yield Simulation.hold, self, 10.0
        assert rl.getTokens() == 10

        # Consume 10 tokens
        self.consumeNTokens(rl, 10)
        assert rl.getTokens() == 0
        yield Simulation.hold, self, 20.0
        assert rl.getTokens() == 5, rl.getTokens()

        # Consume all 5 tokens...
        self.consumeNTokens(rl, 5)
        assert rl.getTokens() == 0
        # ... and now try to consume another one
        self.consumeNTokens(rl, 1)
        assert rl.getTokens() == 0

    def test2(self):
        yield Simulation.hold, self,
        rl = client.RateLimiter("RL-1", self, 10, 20)
        rl.rate = 5   # Five tokens per 20ms
        rl.tokens = 10
        assert rl.getTokens() == 10

        # Let's now force consume tokens to simulate
        # repair activity
        rl.tryAcquire()
        rl.tokens = 10
        rl.lastSent = Simulation.now()
        self.forceConsumeNTokens(rl, 30)
        assert rl.getTokens() == -20, rl.getTokens()

        # No juice.
        self.consumeNTokens(rl, 100)
        assert rl.getTokens() == -20
        yield Simulation.hold, self, 20.0

        # Tokens update despite negative waiting times
        assert rl.getTokens() == -15

        # Test waiting time accuracy.
        # The number of tryAcquires() should not affect
        # the waiting time.
        timeToWait = rl.tryAcquire()
        yield Simulation.hold, self, timeToWait - 1.1
        assert rl.getTokens() > 0 and rl.getTokens() < 1

        # ... test for side-effects
        self.consumeNTokens(rl, 100)
        assert rl.getTokens() > 0 and rl.getTokens() < 1.0

        timeToWait = rl.tryAcquire()
        assert timeToWait > 0.09 and timeToWait <= 1.1, timeToWait
        yield Simulation.hold, self, timeToWait

    def test3(self):
        yield Simulation.hold, self,
        rl = client.RateLimiter("RL-1", self, 10, 20)
        rl.rate = 5   # Five tokens per 20ms
        rl.tokens = 10
        assert rl.getTokens() == 10
        self.consumeNTokens(rl, 10)
        assert rl.getTokens() == 0

        timeToWait1 = rl.tryAcquire()
        # We're now at zero tokens. The time
        # to reach one token is 4ms.
        assert timeToWait1 == 4.0, timeToWait1
        yield Simulation.hold, self, 1

        timeToWait2 = rl.tryAcquire()
        assert timeToWait2 == 3.0

        self.forceConsumeNTokens(rl, 1)
        timeToWait3 = rl.tryAcquire()
        assert timeToWait3 == 8.0, timeToWait3


class RateLimiterTest(unittest.TestCase):

    def testRateLimiter1(self):
        Simulation.initialize()
        observer = Observer()
        Simulation.activate(observer, observer.test1(), at=0.1)
        Simulation.simulate(until=200)

    def testRateLimiter2(self):
        Simulation.initialize()
        observer = Observer()
        Simulation.activate(observer, observer.test2(), at=0.1)
        Simulation.simulate(until=200)

    def testRateLimiter3(self):
        Simulation.initialize()
        observer = Observer()
        Simulation.activate(observer, observer.test3(), at=0.1)
        Simulation.simulate(until=200)


if __name__ == '__main__':
    unittest.main()
