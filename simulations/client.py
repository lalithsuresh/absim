import SimPy.Simulation as Simulation
import random
import numpy
import constants


class Client():
    def __init__(self, id_, serverList, replicaSelectionStrategy,
                 accessPattern, replicationFactor):
        self.id = id_
        self.serverList = serverList
        self.pendingRequestsMap = {node: 0 for node in serverList}
        self.pendingXserviceMap = {node: 0 for node in serverList}
        self.responseTimesMap = {node: 0 for node in serverList}
        self.taskTimeTracker = {}
        self.accessPattern = accessPattern
        self.replicationFactor = replicationFactor
        self.REPLICA_SELECTION_STRATEGY = replicaSelectionStrategy
        self.pendingRequestsMonitor = Simulation.Monitor(name="PendingRequests")
        self.latencyTrackerMonitor = Simulation.Monitor(name="LatencyTracker")

    def schedule(self, task):

        replicaSet = None
        replicaToServe = None
        firstReplicaIndex = None

        # Pick a random node and it's next RF - 1 number of neighbours
        if (self.accessPattern == "uniform"):
            firstReplicaIndex = random.randint(0, len(self.serverList) - 1)
        elif(self.accessPattern == "zipfian"):
            firstReplicaIndex = numpy.random.zipf(2) % len(self.serverList)

        replicaSet = [self.serverList[i % len(self.serverList)]
                      for i in range(firstReplicaIndex,
                                     firstReplicaIndex +
                                     self.replicationFactor)]
        sortedReplicaSet = self.sort(replicaSet)
        replicaToServe = sortedReplicaSet[0]

        delay = constants.NW_LATENCY_BASE + \
            random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)
        startTime = Simulation.now()
        self.taskTimeTracker[task] = startTime

        messageDeliveryProcess = DeliverMessageWithDelay()
        Simulation.activate(messageDeliveryProcess,
                            messageDeliveryProcess.run(task,
                                                       delay,
                                                       replicaToServe),
                            at=Simulation.now())

        latencyTracker = LatencyTracker()
        Simulation.activate(latencyTracker,
                            latencyTracker.run(self, task, replicaToServe),
                            at=Simulation.now())
        self.pendingRequestsMap[replicaToServe] += 1
        self.pendingXserviceMap[replicaToServe] = \
            (1 + self.pendingRequestsMap[replicaToServe]) \
            * replicaToServe.serviceTime
        self.pendingRequestsMonitor.observe(
            "%s %s" % (replicaToServe.id,
                       self.pendingRequestsMap[replicaToServe]))

    def sort(self, originalReplicaSet):

        replicaSet = originalReplicaSet[0:]

        if(self.REPLICA_SELECTION_STRATEGY == "random"):
            # Pick a random node for the request.
            # Represents SimpleSnitch + uniform request access.
            # Ignore scores and everything else.
            random.shuffle(replicaSet)

        elif(self.REPLICA_SELECTION_STRATEGY == "pending"):
            # Sort by number of pending requests
            replicaSet.sort(key=self.pendingRequestsMap.get)
        elif(self.REPLICA_SELECTION_STRATEGY == "response_time"):
            # Sort by response times
            replicaSet.sort(key=self.responseTimesMap.get)
        elif(self.REPLICA_SELECTION_STRATEGY == "primary"):
            pass
        elif(self.REPLICA_SELECTION_STRATEGY == "pendingXservice_time"):
            # Sort by response times * pending-requests
            replicaSet.sort(key=self.pendingXserviceMap.get)
        else:
            print self.REPLICA_SELECTION_STRATEGY
            assert False, "REPLICA_SELECTION_STRATEGY isn't set or is invalid"

        return replicaSet


class DeliverMessageWithDelay(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='DeliverMessageWithDelay')

    def run(self, task, delay, replicaToServe):
        yield Simulation.hold, self, delay
        replicaToServe.enqueueTask(task)


class LatencyTracker(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='LatencyTracker')

    def run(self, client, task, replicaToServe):
        yield Simulation.hold, self,
        yield Simulation.waitevent, self, task.completionEvent

        delay = constants.NW_LATENCY_BASE + \
            random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)
        yield Simulation.hold, self, delay

        # OMG request completed
        client.pendingRequestsMap[replicaToServe] -= 1
        client.pendingXserviceMap[replicaToServe] = \
            (1 + client.pendingRequestsMap[replicaToServe]) \
            * replicaToServe.serviceTime
        client.responseTimesMap[replicaToServe] = \
            Simulation.now() - client.taskTimeTracker[task]
        client.latencyTrackerMonitor\
              .observe("%s %s" % (replicaToServe.id,
                       client.responseTimesMap[replicaToServe]))
        del client.taskTimeTracker[task]
        task.latencyMonitor.observe(Simulation.now() - task.start)
