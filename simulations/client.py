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
        self.rateMonitor = Simulation.Monitor(name="AlphaMonitor")
        self.muMaxMonitor = Simulation.Monitor(name="MuMaxMonitor")
        self.tokenMonitor = Simulation.Monitor(name="TokenMonitor")
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
        self.expectedDelayMap = {node: {} for node in serverList}
        self.expectedDelayPerTask = {}

        # Rate limiters per replica
        self.rateLimiters = {node: RateLimiter("RL-%s" % node.id, self, 10)
                             for node in serverList}
        self.lastRateDecrease = {node: 0 for node in serverList}
        self.valueOfLastDecrease = {node: 10 for node in serverList}

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
        if (len(self.expectedDelayMap[replica]) != 0):
            metricMap = self.expectedDelayMap[replica]
            twiceNetworkLatency = metricMap["responseTime"]\
                - (metricMap["serviceTime"] + metricMap["waitingTime"])
            total += (twiceNetworkLatency +
                      (1 + self.pendingRequestsMap[replica]
                      + metricMap["queueSizeAfter"])
                      * metricMap["serviceTime"])
        else:
            if (len(self.outstandingRequests) != 0):
                sentTime = self.taskSentTimeTracker[self.outstandingRequests[0]]
                return sentTime
            else:
                return 0
        return total

    def maybeSendShadowReads(self, replicaToServe, replicaSet):
        if (random.uniform(0, 1.0) < self.shadowReadRatio):
            for replica in replicaSet:
                if (replica is not replicaToServe):
                    shadowReadTask = task.Task("ShadowRead", None)
                    self.taskArrivalTimeTracker[shadowReadTask] =\
                        Simulation.now()
                    self.taskSentTimeTracker[shadowReadTask] = Simulation.now()
                    self.sendRequest(shadowReadTask, replica)
                    self.rateLimiters[replica].update()

    def updateEma(self, replica, metricMap):
        alpha = 0.90
        if (len(self.expectedDelayMap[replica]) == 0):
            self.expectedDelayMap[replica] = metricMap
            return

        for metric in metricMap:
            self.expectedDelayMap[replica][metric] \
                = alpha * metricMap[metric] + (1 - alpha) \
                * self.expectedDelayMap[replica][metric]

    def updateRates(self, replica, metricMap, task):
        mus = []
        for server in self.serverList:
            totalMu = 0
            if (len(self.expectedDelayMap[server]) != 0):
                totalMu = self.expectedDelayMap[server]["serviceTime"] \
                    + self.expectedDelayMap[server]["waitingTime"]
            mus.append(totalMu)

        self.muMax = (0.2) * numpy.max(mus) + (1 - 0.2) * self.muMax
        self.muMaxMonitor.observe(self.muMax)

        shuffledNodeList = self.serverList[0:]
        random.shuffle(shuffledNodeList)
        for node in shuffledNodeList:
            self.backpressureSchedulers[node].congestionEvent.signal()

        # CUBIC CONSTANTS
        beta = 0.2
        C = 0.000004
        Smax = 20
        # if (self.muMax > expDelay):
        if (self.muMax > self.expectedDelayPerTask[task]):
            T = Simulation.now() - \
                self.lastRateDecrease[replica]
            Wmax = self.valueOfLastDecrease[replica]
            oldValue = self.rateLimiters[replica].rate
            newValue = C * (T - (Wmax * beta/C)**(1.0/3.0))**3 + Wmax
            if (newValue - oldValue > Smax):
                self.rateLimiters[replica].rate += Smax
            else:
                self.rateLimiters[replica].rate = newValue
        elif (self.muMax < self.expectedDelayPerTask[task]):
            self.valueOfLastDecrease[replica] = \
                self.rateLimiters[replica].rate
            self.rateLimiters[replica].rate *= beta
            self.rateLimiters[replica].rate = \
                max(self.rateLimiters[replica].rate, 0.0001)
            self.lastRateDecrease[replica] = Simulation.now()

        if (self.rateLimiters[replica].rate < 0):
            self.rateLimiters[replica].rate = 0
        alphaObservation = (replica.id,
                            self.rateLimiters[replica].rate)
        self.rateMonitor.observe("%s %s" % alphaObservation)
        del self.expectedDelayPerTask[task]


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
        metricMap = task.completionEvent.signalparam
        metricMap["responseTime"] = client.responseTimesMap[replicaThatServed]
        client.updateEma(replicaThatServed, metricMap)

        # TODO: Threshold
        # if (len(client.expectedDelayMap[replicaThatServed])
        #    > client.movingAverageWindow):
        #     client.expectedDelayMap[replicaThatServed].pop(0)

        # Backpressure related book-keeping
        if (client.backpressure):
            client.updateRates(replicaThatServed, metricMap, task)

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

                for replica in sortedReplicaSet:
                    self.client.tokenMonitor.observe("%s %s" \
                        % (replica.id,
                           self.client.rateLimiters[replica].tokens))
                    if (self.client.rateLimiters[replica].tryAcquire()
                       is True):
                        self.backlogQueue.pop(0)
                        self.client.sendRequest(task, replica)
                        self.client.maybeSendShadowReads(replica, replicaSet)
                        sent = True
                        self.client.rateLimiters[replica].update()
                        self.client.expectedDelayPerTask[task] = \
                            self.client.computeExpectedDelay(replica)
                        break

                if (not sent):
                    print 'conge', Simulation.now()
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
        self.rate = 1
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
                yield Simulation.hold, self, 1/float(self.rate)
                self.tokens += 1
                shuffledNodeList = self.client.serverList[0:]
                random.shuffle(shuffledNodeList)
                for node in shuffledNodeList:
                    self.client \
                        .backpressureSchedulers[node].congestionEvent.signal()

    # These updates can be forced due to shadowReads
    def update(self):
        self.lastSent = Simulation.now()
        self.tokens -= 1
        if (self.tokens <= 0):
            self.waitIfZeroTokens.signal()

    def tryAcquire(self):
        self.tokens = min(self.maxTokens, self.tokens
                          + self.rate * (Simulation.now() - self.lastSent))
        print Simulation.now(), self.tokens, self.rate
        return self.tokens >= 1
