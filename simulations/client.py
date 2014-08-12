import SimPy.Simulation as Simulation
import random
import numpy
import constants
import task


class Client():
    def __init__(self, id_, serverList, replicaSelectionStrategy,
                 accessPattern, replicationFactor, backpressure,
                 shadowReadRatio):
        self.id = id_
        self.serverList = serverList
        self.accessPattern = accessPattern
        self.replicationFactor = replicationFactor
        self.REPLICA_SELECTION_STRATEGY = replicaSelectionStrategy
        self.pendingRequestsMonitor = Simulation.Monitor(name="PendingRequests")
        self.latencyTrackerMonitor = Simulation.Monitor(name="ResponseHandler")
        self.alphaMonitor = Simulation.Monitor(name="AlphaMonitor")
        self.movingAverageWindow = 10
        self.backpressure = backpressure    # True/Flase
        self.shadowReadRatio = shadowReadRatio

        # Book-keeping and metrics to be recorded follow...

        # Number of outstanding requests at the client
        self.pendingRequestsMap = {node: 0 for node in serverList}

        # Number of outstanding requests times oracle-service time of replica
        self.pendingXserviceMap = {node: 0 for node in serverList}

        # Last-received response time of server
        self.responseTimesMap = {node: 0 for node in serverList}

        # Used to track response time from the perspective of the client
        self.taskSentTimeTracker = {}
        self.taskArrivalTimeTracker = {}

        # This is needed to prevent the initial state from blowing up
        # Probably not needed on an actual implementation
        self.outstandingRequests = []

        # Record waiting and service times as relayed by the server
        self.expectedDelayMap = {node: [] for node in serverList}

        # Rate limiters per replica
        self.rateLimiters = {node: RateLimiter("RL-%s" % node.id, self, 10)
                             for node in serverList}
        self.lastRateDecrease = {node: 0 for node in serverList}
        self.valueOfLastDecrease = {node: 1 for node in serverList}

        # Backpressure related initialization
        self.backpressureSchedulers =\
            {node: BackpressureScheduler("BP%s" % (node.id), self)
                for node in serverList}

        self.muMax = 0.0

        for node, sched in self.backpressureSchedulers.items():
            Simulation.activate(sched, sched.run(), at=Simulation.now())

        for node, rateLimiter in self.rateLimiters.items():
            Simulation.activate(rateLimiter, rateLimiter.run(),
                                at=Simulation.now())

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
        startTime = Simulation.now()
        self.taskArrivalTimeTracker[task] = startTime

        if(self.backpressure is False):
            sortedReplicaSet = self.sort(replicaSet)
            replicaToServe = sortedReplicaSet[0]
            self.sendRequest(task, replicaToServe)
            self.maybeSendShadowReads(replicaToServe, replicaSet)
        else:
            firstReplica = self.serverList[firstReplicaIndex]
            self.backpressureSchedulers[firstReplica].enqueue(task, replicaSet)

    def sendRequest(self, task, replicaToServe):
        delay = constants.NW_LATENCY_BASE + \
            random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)

        # Immediately send out request
        messageDeliveryProcess = DeliverMessageWithDelay()
        Simulation.activate(messageDeliveryProcess,
                            messageDeliveryProcess.run(task,
                                                       delay,
                                                       replicaToServe),
                            at=Simulation.now())

        responseHandler = ResponseHandler()
        Simulation.activate(responseHandler,
                            responseHandler.run(self, task, replicaToServe),
                            at=Simulation.now())

        # Book-keeping for metrics
        self.pendingRequestsMap[replicaToServe] += 1
        self.pendingXserviceMap[replicaToServe] = \
            (1 + self.pendingRequestsMap[replicaToServe]) \
            * replicaToServe.serviceTime
        self.pendingRequestsMonitor.observe(
            "%s %s" % (replicaToServe.id,
                       self.pendingRequestsMap[replicaToServe]))
        self.taskSentTimeTracker[task] = Simulation.now()
        self.outstandingRequests.append(task)

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
        elif(self.REPLICA_SELECTION_STRATEGY == "pendingXserviceTime"):
            # Sort by response times * client-local-pending-requests
            replicaSet.sort(key=self.pendingXserviceMap.get)
        elif(self.REPLICA_SELECTION_STRATEGY == "pendingXserviceTimeOracle"):
            # Sort by response times * pending-requests
            oracleMap = {replica: (1 + len(replica.queueResource.activeQ
                                   + replica.queueResource.waitQ))
                         * replica.serviceTime
                         for replica in originalReplicaSet}
            replicaSet.sort(key=oracleMap.get)
        elif(self.REPLICA_SELECTION_STRATEGY == "expDelay"):
            sortMap = {}
            for replica in originalReplicaSet:
                sortMap[replica] = self.computeExpectedDelay(replica)
            replicaSet.sort(key=sortMap.get)
        else:
            print self.REPLICA_SELECTION_STRATEGY
            assert False, "REPLICA_SELECTION_STRATEGY isn't set or is invalid"

        return replicaSet

    def computeExpectedDelay(self, replica):
        total = 0
        for entry in self.expectedDelayMap[replica]:
            twiceNetworkLatency = entry["responseTime"]\
                - (entry["serviceTime"] + entry["waitingTime"])
            total += (twiceNetworkLatency +
                      (1 + self.pendingRequestsMap[replica]
                      * constants.NUMBER_OF_CLIENTS)
                      * entry["serviceTime"])
            # total += entry["waitingTime"]
        numberOfEntries = float(len(self.expectedDelayMap[replica]))
        if (numberOfEntries == 0):
            if (len(self.outstandingRequests) != 0):
                sentTime = self.taskSentTimeTracker[self.outstandingRequests[0]]
                return sentTime
            else:
                return 0
        return total/numberOfEntries

    def maybeSendShadowReads(self, replicaToServe, replicaSet):
        if (random.uniform(0, 1.0) < self.shadowReadRatio):
            for replica in replicaSet:
                if (replica is not replicaToServe):
                    shadowReadTask = task.Task("ShadowRead", None)
                    self.taskTimeSentTracker[shadowReadTask] = Simulation.now()
                    self.sendRequest(shadowReadTask, replica)


class DeliverMessageWithDelay(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='DeliverMessageWithDelay')

    def run(self, task, delay, replicaToServe):
        yield Simulation.hold, self, delay
        replicaToServe.enqueueTask(task)


class ResponseHandler(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='ResponseHandler')

    def run(self, client, task, replicaThatServed):
        yield Simulation.hold, self,
        yield Simulation.waitevent, self, task.completionEvent

        delay = constants.NW_LATENCY_BASE + \
            random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)
        yield Simulation.hold, self, delay

        # OMG request completed. Time for some book-keeping
        client.outstandingRequests.remove(task)
        client.pendingRequestsMap[replicaThatServed] -= 1
        client.pendingXserviceMap[replicaThatServed] = \
            (1 + client.pendingRequestsMap[replicaThatServed]) \
            * replicaThatServed.serviceTime

        client.pendingRequestsMonitor.observe(
            "%s %s" % (replicaThatServed.id,
                       client.pendingRequestsMap[replicaThatServed]))

        client.responseTimesMap[replicaThatServed] = \
            Simulation.now() - client.taskSentTimeTracker[task]
        client.latencyTrackerMonitor\
              .observe("%s %s" % (replicaThatServed.id,
                       Simulation.now() - client.taskSentTimeTracker[task]))
        expDelayMap = task.completionEvent.signalparam
        expDelayMap["responseTime"] = client.responseTimesMap[replicaThatServed]
        client.expectedDelayMap[replicaThatServed]\
            .append(expDelayMap)

        # TODO: Threshold
        if (len(client.expectedDelayMap[replicaThatServed])
           > client.movingAverageWindow):
            client.expectedDelayMap[replicaThatServed].pop(0)

        # Backpressure related book-keeping
        if (client.backpressure):
            mus = []
            for replica in client.serverList:
                totalMu = sum([entry.get("serviceTime") +
                               entry.get("waitingTime")
                              for entry in client.expectedDelayMap[replica]])
                numberOfEntries = float(len(client.expectedDelayMap[replica]))
                meanMu = 0 if numberOfEntries == 0.0 \
                    else totalMu/numberOfEntries
                mus.append(meanMu)

            client.muMax = max(mus)

            shuffledNodeList = client.serverList[0:]
            random.shuffle(shuffledNodeList)
            for node in shuffledNodeList:
                client.backpressureSchedulers[node].congestionEvent.signal()

            expDelay = client.computeExpectedDelay(replicaThatServed)

            if (client.muMax > expDelay):
                client.valueOfLastDecrease[replicaThatServed] = \
                    client.rateLimiters[replicaThatServed].alpha
                client.rateLimiters[replicaThatServed].alpha *= 0.2
                client.rateLimiters[replicaThatServed].alpha = \
                    max(client.rateLimiters[replicaThatServed].alpha, 0.0001)
                client.lastRateDecrease[replicaThatServed] = Simulation.now()
            elif (client.muMax < expDelay):
                # C * (T - (Wmax * Beta/C)^(1/3)) ^ 3 + Wmax
                beta = 0.2
                C = 0.00004
                Smax = 10
                T = Simulation.now() - \
                    client.lastRateDecrease[replicaThatServed]
                Wmax = client.valueOfLastDecrease[replicaThatServed]
                oldValue = client.rateLimiters[replicaThatServed].alpha
                newValue = C * (T - (Wmax * beta/C)**(1.0/3.0))**3 + Wmax
                if (newValue - oldValue > Smax):
                    client.rateLimiters[replicaThatServed].alpha += Smax
                else:
                    client.rateLimiters[replicaThatServed].alpha = newValue

            if (client.rateLimiters[replicaThatServed].alpha < 0):
                client.rateLimiters[replicaThatServed].alpha = 0
            alphaObservation = (replicaThatServed.id,
                                client.rateLimiters[replicaThatServed].alpha)
            client.alphaMonitor.observe("%s %s" % alphaObservation)

        del client.taskSentTimeTracker[task]
        del client.taskArrivalTimeTracker[task]

        # Does not make sense to record shadow read latencies
        # as a latency measurement
        if (task.latencyMonitor is not None):
            task.latencyMonitor.observe(Simulation.now() - task.start)


class BackpressureScheduler(Simulation.Process):
    def __init__(self, id_, client):
        self.id = id_
        self.backlogQueue = []
        self.client = client
        self.congestionEvent = Simulation.SimEvent("Congestion")
        self.backlogReadyEvent = Simulation.SimEvent("BacklogReady")
        Simulation.Process.__init__(self, name='BackpressureScheduler')

    def run(self):
        while(1):
            yield Simulation.hold, self,
            if (len(self.backlogQueue) != 0):
                task, replicaSet = self.backlogQueue[0]
                sortedReplicaSet = self.client.sort(replicaSet)
                sent = False

                if(random.uniform(0, 1) < 0.10):
                    sortedReplicaSet[0], sortedReplicaSet[1] = \
                        sortedReplicaSet[1], sortedReplicaSet[0]

                for replica in sortedReplicaSet:
                    if (self.client.rateLimiters[replica].tryAcquire()
                       is True):
                        self.backlogQueue.pop(0)
                        self.client.sendRequest(task, replica)
                        self.client.maybeSendShadowReads(replica, replicaSet)
                        sent = True
                        self.client.rateLimiters[replica].update()
                        break

                if (not sent):
                    yield Simulation.waitevent, self, self.congestionEvent
                    self.congestionEvent = Simulation.SimEvent("Congestion")
            else:
                yield Simulation.waitevent, self, self.backlogReadyEvent
                self.backlogReadyEvent = Simulation.SimEvent("BacklogReady")

    def enqueue(self, task, replicaSet):
        self.backlogQueue.append((task, replicaSet))
        self.backlogReadyEvent.signal()


# TODO: does not ramp up or account for underutilization
class RateLimiter(Simulation.Process):
    def __init__(self, id_, client, maxTokens):
        self.id = id_
        self.alpha = 10
        self.lastSent = 0
        self.client = client
        self.tokens = 0
        self.maxTokens = maxTokens
        self.waitIfZeroTokens = Simulation.SimEvent("WaitIfZeroTokens")
        Simulation.Process.__init__(self, name='RateLimiter')

    def run(self):
        while (1):
            yield Simulation.hold, self

            if (self.tokens != 0):
                yield Simulation.waitevent, self, self.waitIfZeroTokens
                self.waitIfZeroTokens = Simulation.SimEvent("BacklogReady")
            else:
                yield Simulation.hold, self, self.alpha/100.0
                self.tokens += 1
                shuffledNodeList = self.client.serverList[0:]
                random.shuffle(shuffledNodeList)
                for node in shuffledNodeList:
                    self.client \
                        .backpressureSchedulers[node].congestionEvent.signal()

    def update(self):
        self.lastSent = Simulation.now()
        self.tokens -= 1
        if (self.tokens == 0):
            self.waitIfZeroTokens.signal()

    def tryAcquire(self):
        rate = 1/float((self.alpha)/100.0)
        self.tokens = self.tokens + rate * (Simulation.now() - self.lastSent)
        self.tokens = self.maxTokens if self.tokens >= self.maxTokens \
            else self.tokens
        if (self.tokens >= 1):
            return True
        else:
            return False
