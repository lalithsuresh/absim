import SimPy.Simulation as Simulation
import random
import numpy
import constants
import task
import math
import sys
from node import Node
from yunomi.stats.exp_decay_sample import ExponentiallyDecayingSample


class Client(Node):
    def __init__(self, id_, serverList, replicaSelectionStrategy,
                 accessPattern, replicationFactor, backpressure,
                 shadowReadRatio, rateInterval,
                 cubicC, cubicSmax, cubicBeta, hysterisisFactor,
                 demandWeight):
        Node.__init__(self, id_, "client")
        self.id = id_
        self.serverList = serverList
        self.accessPattern = accessPattern
        self.replicationFactor = replicationFactor
        self.REPLICA_SELECTION_STRATEGY = replicaSelectionStrategy
        self.pendingRequestsMonitor = Simulation.Monitor(name="PendingRequests")
        self.latencyTrackerMonitor = Simulation.Monitor(name="ResponseHandler")
        self.rateMonitor = Simulation.Monitor(name="AlphaMonitor")
        self.receiveRateMonitor = Simulation.Monitor(name="ReceiveRateMonitor")
        self.tokenMonitor = Simulation.Monitor(name="TokenMonitor")
        self.edScoreMonitor = Simulation.Monitor(name="edScoreMonitor")
        self.qErrorMonitor = Simulation.Monitor(name="QueueErrorMonitor")
        self.selErrorMonitor = Simulation.Monitor(name="SelectionErrorMonitor")
        self.backlogMonitor = Simulation.Monitor(name="BacklogMonitor")
        self.pdf = Simulation.Monitor(name="")
        self.backpressure = backpressure    # True/Flase
        self.shadowReadRatio = shadowReadRatio
        self.demandWeight = demandWeight
        self.responsesReceived = 0
        self.requestStatus = {} #request/response mappings to keep track of a request's status (in terms of received packets)
        self.request = {} #mapping of request ID to request object
        self.pendingRequestsPerServer = {node: Simulation.Monitor(name="PendingRequests"+str(node.id)) for node in serverList}
        self.queueSizePerServer = {node: Simulation.Monitor(name="PendingRequests"+str(node.id)) for node in serverList}

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

        # Record waiting and service times as relayed by the server
        self.expectedDelayMap = {node: {} for node in serverList}
        self.lastSeen = {node: 0 for node in serverList}

        # Rate limiters per replica
        self.rateLimiters = {node: RateLimiter("RL-%s" % node.id,
                                               self, 50, rateInterval)
                             for node in serverList}
        self.lastRateDecrease = {node: 0 for node in serverList}
        self.valueOfLastDecrease = {node: 10 for node in serverList}
        self.receiveRate = {node: ReceiveRate("RL-%s" % node.id, rateInterval)
                            for node in serverList}
        self.lastRateIncrease = {node: 0 for node in serverList}
        self.rateInterval = rateInterval

        # Parameters for congestion control
        self.cubicC = cubicC
        self.cubicSmax = cubicSmax
        self.cubicBeta = cubicBeta
        self.hysterisisFactor = hysterisisFactor

        # Backpressure related initialization
        if (backpressure is True):
            self.backpressureSchedulers = \
                {node: BackpressureScheduler("BP-%s" % node.id, self)
                 for node in serverList}
            for node in serverList:
                Simulation.activate(self.backpressureSchedulers[node],
                                    self.backpressureSchedulers[node].run(),
                                    at=Simulation.now())

        # ds-metrics
        if (replicaSelectionStrategy == "ds"):
            self.latencyEdma = {node: ExponentiallyDecayingSample(100,
                                                                  0.75,
                                                                  self.clock)
                                for node in serverList}
            self.dsScores = {node: 0 for node in serverList}
            for node, rateLimiter in self.rateLimiters.items():
                ds = DynamicSnitch(self, 100)
                Simulation.activate(ds, ds.run(),
                                    at=Simulation.now())

    def clock(self):
        '''
            Convert to seconds because that's what the
            ExponentiallyDecayingSample
            assumes. Else, the internal Math.exp overflows.
        '''
        return Simulation.now()/1000.0

    def schedule(self, task, replicaSet=None):
        #print self.id, 'is sending request:', task.id
        replicaToServe = None
        firstReplicaIndex = None

        # Pick a random node and it's next RF - 1 number of neighbours
        if (self.accessPattern == "uniform"):
            firstReplicaIndex = random.randint(0, len(self.serverList) - 1)
        elif(self.accessPattern == "zipfian"):
            firstReplicaIndex = numpy.random.zipf(1.5) % len(self.serverList)

        if (replicaSet is None):
            replicaSet = [self.serverList[i % len(self.serverList)]
                          for i in range(firstReplicaIndex,
                                         firstReplicaIndex +
                                         self.replicationFactor)]
        startTime = Simulation.now()
        self.taskArrivalTimeTracker[task] = startTime

        if(self.backpressure is False):
            sortedReplicaSet = self.sort(replicaSet)
            replicaToServe = sortedReplicaSet[0]
            if (len(self.expectedDelayMap[replicaToServe]) != 0):
                queueSizeEst = self.pendingRequestsMap[replicaToServe] \
                        * constants.NUMBER_OF_CLIENTS \
                        + self.expectedDelayMap[replicaToServe]["queueSizeAfter"]
            else:
                queueSizeEst = 0
            task.addQueueSizeEst(queueSizeEst)
            task.addReplicaSet(sortedReplicaSet)
            self.sendRequest(task, replicaToServe, False)
            self.maybeSendShadowReads(replicaToServe, replicaSet)
        else:
            self.backpressureSchedulers[replicaSet[0]].enqueue(task, replicaSet)

    def sendRequest(self, task, replicaToServe, retransmit):
        task.setDestination(replicaToServe)
        # Map request ID to request object
        self.request[task.id] = task    
        # Add response packets to be received
        self.requestStatus[task] = [i for i in xrange(1, task.count+1)]
        # Get switch I'm delivering to
        nextSwitch = self.getNeighbors().keys()[0]
        # Get port I'm delivering through
        egress = self.getPort(nextSwitch)

        # Immediately send out request
        egress.enqueueTask(task)

        # Set timer for packet
        #packetTimer = PacketTimer()
        #Simulation.activate(packetTimer,
        #                    packetTimer.run(self, task, replicaToServe),
        #                    at=Simulation.now())

        # Book-keeping for metrics
        self.pendingRequestsMap[replicaToServe] += 1
        self.pendingXserviceMap[replicaToServe] = \
            (1 + self.pendingRequestsMap[replicaToServe]) \
            * replicaToServe.serviceTime
        self.pendingRequestsMonitor.observe(
            "%s %s" % (replicaToServe.id,
                       self.pendingRequestsMap[replicaToServe]))
        if not retransmit:
            self.taskSentTimeTracker[task] = Simulation.now()
        #self.taskSentTimeTracker[task] = Simulation.now()

    def sort(self, originalReplicaSet, forceStrategy=False):

        replicaSet = originalReplicaSet[0:]
        if(forceStrategy):
            selectionStrategy = forceStrategy
        else:
            selectionStrategy = self.REPLICA_SELECTION_STRATEGY
        if(selectionStrategy == "random"):
            # Pick a random node for the request.
            # Represents SimpleSnitch + uniform request access.
            # Ignore scores and everything else.
            random.shuffle(replicaSet)

        elif(selectionStrategy == "pending"):
            # Sort by number of pending requests
            replicaSet.sort(key=self.pendingRequestsMap.get)
        elif(selectionStrategy == "response_time"):
            # Sort by response times
            replicaSet.sort(key=self.responseTimesMap.get)
        elif(selectionStrategy == "weighted_response_time"):
            # Weighted random proportional to response times
            m = {}
            for each in replicaSet:
                if ("serviceTime" not in self.expectedDelayMap[each]):
                    m[each] = 0.0
                else:
                    m[each] = self.expectedDelayMap[each]["serviceTime"]
            replicaSet.sort(key=m.get)
            total = sum(map(lambda x: self.responseTimesMap[x], replicaSet))
            selection = random.uniform(0, total)
            cumSum = 0
            nodeToSelect = None
            i = 0
            if (total != 0):
                for entry in replicaSet:
                    cumSum += self.responseTimesMap[entry]
                    if (selection < cumSum):
                        nodeToSelect = entry
                        break
                    i += 1
                assert nodeToSelect is not None

                replicaSet[0], replicaSet[i] = replicaSet[i], replicaSet[0]
        elif(selectionStrategy == "primary"):
            pass
        elif(selectionStrategy == "pendingXserviceTime"):
            # Sort by response times * client-local-pending-requests
            replicaSet.sort(key=self.pendingXserviceMap.get)
        elif(selectionStrategy == "clairvoyant"):
            # Sort by response times * pending-requests
            oracleMap = {replica: (1 + len(replica.queueResource.activeQ
                                   + replica.queueResource.waitQ))
                         * replica.serviceTime
                         for replica in originalReplicaSet}
            replicaSet.sort(key=oracleMap.get)
        elif(selectionStrategy == "expDelay"):
            sortMap = {}
            for replica in originalReplicaSet:
                sortMap[replica] = self.computeExpectedDelay(replica)
            replicaSet.sort(key=sortMap.get)
        elif(selectionStrategy == "ds"):
            firstNode = replicaSet[0]
            firstNodeScore = self.dsScores[firstNode]
            badnessThreshold = 0.0

            if (firstNodeScore != 0.0):
                for node in replicaSet[1:]:
                    newNodeScore = self.dsScores[node]
                    if ((firstNodeScore - newNodeScore)/firstNodeScore
                       > badnessThreshold):
                        replicaSet.sort(key=self.dsScores.get)
        else:
            print selectionStrategy
            assert False, "REPLICA_SELECTION_STRATEGY isn't set or is invalid"

        return replicaSet

    def receiveResponse(self, packet):
        if(packet.isCut):
            #This is a notification of a packet drop
            #Resend packet
            #TODO We actually need to reschedule the request so that it passes through the backpressure scheudler
            self.sendRequest(self.request[packet.id], packet.src, True)
            return
        #print 'Client received response with ID:', packet.id, 'seqN:', packet.seqN, 'count:', packet.count
        #print 'Client has', len(self.requestStatus.keys()), 'pending requests'
        if self.request[packet.id] in self.requestStatus:
            #print packet.id, self.requestStatus[packet.id]
            status = self.requestStatus[self.request[packet.id]]
            status.remove(packet.seqN)
            if(len(status)==0):
                #Response received in full
                #print self.id, 'successfully received response for request:', packet.id
                self.updateStats(self.request[packet.id], packet.src)
        else:
            print 'Something wrong happend!'
            sys.exit(-1)
                
    def metricDecay(self, replica):
        return math.exp(-(Simulation.now() - self.lastSeen[replica])
                        / (2 * self.rateInterval))

    def computeExpectedDelay(self, replica):
        total = 0
        if (len(self.expectedDelayMap[replica]) != 0):
            metricMap = self.expectedDelayMap[replica]
            twiceNetworkLatency = metricMap["nw"]
            theta = (1 + self.pendingRequestsMap[replica]
                     * constants.NUMBER_OF_CLIENTS
                     + metricMap["queueSizeAfter"])
            total += (twiceNetworkLatency +
                      ((theta ** 3) * (metricMap["serviceTime"])))
            self.edScoreMonitor.observe("%s %s %s %s %s" %
                                        (replica.id,
                                         metricMap["queueSizeAfter"],
                                         metricMap["serviceTime"],
                                         theta,
                                         total))
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
                    self.sendRequest(shadowReadTask, replica, False)
                    self.rateLimiters[replica].forceUpdates()

    def updateEma(self, replica, metricMap):
        alpha = 0.9
        if (len(self.expectedDelayMap[replica]) == 0):
            self.expectedDelayMap[replica] = metricMap
            return

        for metric in metricMap:
            if(not (metric == "idealReplicaSet" or metric == "totalQueueSizeBefore")):
                self.expectedDelayMap[replica][metric] \
                    = alpha * metricMap[metric] + (1 - alpha) \
                    * self.expectedDelayMap[replica][metric]
            else:
                self.expectedDelayMap[replica][metric] \
                    = metricMap[metric]

    def updateRates(self, replica, metricMap, task):
        # Cubic Parameters go here
        # beta = 0.2
        # C = 0.000004
        # Smax = 10
        beta = self.cubicBeta
        C = self.cubicC
        Smax = self.cubicSmax
        hysterisisFactor = self.hysterisisFactor
        currentSendingRate = self.rateLimiters[replica].rate
        currentReceiveRate = self.receiveRate[replica].getRate()

        if (currentSendingRate < currentReceiveRate):
            # This means that we need to bump up our own rate.
            # For this, increase the rate according to a cubic
            # window. Rmax is the sending-rate at which we last
            # observed a congestion event. We grow aggressively
            # towards this point, and then slow down, stabilise,
            # and then advance further up. Every rate
            # increase is capped by Smax.
            T = Simulation.now() - self.lastRateDecrease[replica]
            self.lastRateIncrease[replica] = Simulation.now()
            Rmax = self.valueOfLastDecrease[replica]

            newSendingRate = C * (T - (Rmax * beta/C)**(1.0/3.0))**3 + Rmax

            if (newSendingRate - currentSendingRate > Smax):
                self.rateLimiters[replica].rate += Smax
            else:
                self.rateLimiters[replica].rate = newSendingRate
        elif (currentSendingRate > currentReceiveRate
              and Simulation.now() - self.lastRateIncrease[replica]
              > self.rateInterval * hysterisisFactor):
            # The hysterisis factor in the condition is to ensure
            # that the receive-rate measurements have enough time
            # to adapt to the updated rate.

            # So we're in here now, which means we need to back down.
            # Multiplicatively decrease the rate by a factor of beta.
            self.valueOfLastDecrease[replica] = currentSendingRate
            self.rateLimiters[replica].rate *= beta
            self.rateLimiters[replica].rate = \
                max(self.rateLimiters[replica].rate, 0.0001)
            self.lastRateDecrease[replica] = Simulation.now()

        assert (self.rateLimiters[replica].rate > 0)
        alphaObservation = (replica.id,
                            self.rateLimiters[replica].rate)
        receiveRateObs = (replica.id,
                          self.receiveRate[replica].getRate())
        self.rateMonitor.observe("%s %s" % alphaObservation)
        self.receiveRateMonitor.observe("%s %s" % receiveRateObs)

    def updateStats(self, task, replicaThatServed):
        #Lets ignore shadowreads for reporting simulation results
        if(not task.id == "ShadowRead"):
            self.responsesReceived += 1
            
            #Queue Size and outstanding requests book-keeping
            self.pendingRequestsPerServer[replicaThatServed].observe(self.pendingRequestsMap[replicaThatServed]-1, self.taskSentTimeTracker[task])
            self.queueSizePerServer[replicaThatServed].observe(task.completionEvent.signalparam["totalQueueSizeBefore"])
            
            #Scoring-related book-keeping
            replicaSetEst = task.replicaSet
            replicaSetAct = task.completionEvent.signalparam["idealReplicaSet"]
            selectionErrorDist = replicaSetAct.index(replicaSetEst[0])
            self.selErrorMonitor.observe(selectionErrorDist, self.taskSentTimeTracker[task])
            
            #Report queue size relative error (between self and server)
            qEst = task.queueSizeEst
            qAct = task.completionEvent.signalparam["totalQueueSizeBefore"]
            qError = (qEst - qAct)**2
            self.qErrorMonitor.observe("%s %s %s"%(qAct, qEst, qError), self.taskSentTimeTracker[task])

        # OMG request completed. Time for some book-keeping
        self.pendingRequestsMap[replicaThatServed] -= 1
        self.pendingXserviceMap[replicaThatServed] = \
            (1 + self.pendingRequestsMap[replicaThatServed]) \
            * replicaThatServed.serviceTime

        self.pendingRequestsMonitor.observe(
            "%s %s" % (replicaThatServed.id,
                       self.pendingRequestsMap[replicaThatServed]))

        self.responseTimesMap[replicaThatServed] = \
            Simulation.now() - self.taskSentTimeTracker[task]
        self.latencyTrackerMonitor\
              .observe("%s %s" % (replicaThatServed.id,
                       Simulation.now() - self.taskSentTimeTracker[task]))
        metricMap = task.completionEvent.signalparam
        metricMap["responseTime"] = self.responseTimesMap[replicaThatServed]
        metricMap["nw"] = metricMap["responseTime"] - metricMap["serviceTime"]
        self.updateEma(replicaThatServed, metricMap)
        self.receiveRate[replicaThatServed].add(1)

        # Backpressure related book-keeping
        if (self.backpressure):
            self.updateRates(replicaThatServed, metricMap, task)

        self.lastSeen[replicaThatServed] = Simulation.now()

        if (self.REPLICA_SELECTION_STRATEGY == "ds"):
            self.latencyEdma[replicaThatServed]\
                  .update(metricMap["responseTime"])

        del self.taskSentTimeTracker[task]
        del self.taskArrivalTimeTracker[task]
        del self.requestStatus[task]
        del self.request[task.id]
        # Does not make sense to record shadow read latencies
        # as a latency measurement
        if (task.latencyMonitor is not None):
            task.latencyMonitor.observe("%s %s" %
                                        (Simulation.now() - task.start,
                                         self.id))

#===============================================================================
# class PacketTimer(Simulation.Process):
#     def __init__(self):
#         Simulation.Process.__init__(self, name='PacketTimer')
#         
#     def run(self, client, task, replicaThatServed):
#         #We'll wait for packets in a sequential manner (would be problematic in per-packet multipath routing)
#         pendingPacket = min(client.requestStatus[task.id])
#         while True:
#             yield Simulation.hold, self, timeout
#             if pendingPacket in client.requestStatus[task.id]:
#                 #Packet hasn't been received yet --> TIMEOUT
#                 if task.count == len(client.requestStatus[task.id]):
#                     #No packets have been received for this response
#                     #This means that it's likely that entire request wasn't received
#                     #As such, we'll resend the request for the entire value
#                     client.sendRequest(task, replicaThatServed, True)
#                 else:
#                     #We'll only request the specific packet that we timed-out on
#                     #TODO this could cause problems (we should possibly clone this)
#                     task.seqN = pendingPacket
#                     client.sendRequest(task, replicaThatServed, True)
#             else:
#                 #Packet was received; return
#                 return
#===============================================================================

#===============================================================================
# class ResponseHandler(Simulation.Process):
#     def __init__(self):
#         Simulation.Process.__init__(self, name='ResponseHandler')
# 
#     def run(self, client, task, replicaThatServed):
#         yield Simulation.hold, self,
#         yield Simulation.waitevent, self, task.completionEvent
#         
#         #Lets ignore shadowreads for reporting simulation results
#         if(not task.id == "ShadowRead"):
#             client.responsesReceived += 1
#             
#             #Queue Size and outstanding requests book-keeping
#             client.pendingRequestsPerServer[replicaThatServed].observe(client.pendingRequestsMap[replicaThatServed]-1, client.taskSentTimeTracker[task])
#             client.queueSizePerServer[replicaThatServed].observe(task.completionEvent.signalparam["totalQueueSizeBefore"])
#             
#             #Scoring-related book-keeping
#             replicaSetEst = task.replicaSet
#             replicaSetAct = task.completionEvent.signalparam["idealReplicaSet"]
#             selectionErrorDist = replicaSetAct.index(replicaSetEst[0])
#             client.selErrorMonitor.observe(selectionErrorDist, client.taskSentTimeTracker[task])
#             
#             #Report queue size relative error (between client and server)
#             qEst = task.queueSizeEst
#             qAct = task.completionEvent.signalparam["totalQueueSizeBefore"]
#             qError = (qEst - qAct)**2
#             client.qErrorMonitor.observe("%s %s %s"%(qAct, qEst, qError), client.taskSentTimeTracker[task])
#             
#         delay = constants.NW_LATENCY_BASE + \
#             random.normalvariate(constants.NW_LATENCY_MU,
#                                  constants.NW_LATENCY_SIGMA)
#         yield Simulation.hold, self, delay
# 
#         # OMG request completed. Time for some book-keeping
#         client.pendingRequestsMap[replicaThatServed] -= 1
#         client.pendingXserviceMap[replicaThatServed] = \
#             (1 + client.pendingRequestsMap[replicaThatServed]) \
#             * replicaThatServed.serviceTime
# 
#         client.pendingRequestsMonitor.observe(
#             "%s %s" % (replicaThatServed.id,
#                        client.pendingRequestsMap[replicaThatServed]))
# 
#         client.responseTimesMap[replicaThatServed] = \
#             Simulation.now() - client.taskSentTimeTracker[task]
#         client.latencyTrackerMonitor\
#               .observe("%s %s" % (replicaThatServed.id,
#                        Simulation.now() - client.taskSentTimeTracker[task]))
#         metricMap = task.completionEvent.signalparam
#         metricMap["responseTime"] = client.responseTimesMap[replicaThatServed]
#         metricMap["nw"] = metricMap["responseTime"] - metricMap["serviceTime"]
#         client.updateEma(replicaThatServed, metricMap)
#         client.receiveRate[replicaThatServed].add(1)
# 
#         # Backpressure related book-keeping
#         if (client.backpressure):
#             client.updateRates(replicaThatServed, metricMap, task)
# 
#         client.lastSeen[replicaThatServed] = Simulation.now()
# 
#         if (client.REPLICA_SELECTION_STRATEGY == "ds"):
#             client.latencyEdma[replicaThatServed]\
#                   .update(metricMap["responseTime"])
# 
#         del client.taskSentTimeTracker[task]
#         del client.taskArrivalTimeTracker[task]
# 
#         # Does not make sense to record shadow read latencies
#         # as a latency measurement
#         if (task.latencyMonitor is not None):
#             task.latencyMonitor.observe("%s %s" %
#                                         (Simulation.now() - task.start,
#                                          client.id))
#===============================================================================

class BackpressureScheduler(Simulation.Process):
    def __init__(self, id_, client):
        self.id = id_
        self.backlogQueue = []
        self.client = client
        self.count = 0
        self.backlogReadyEvent = Simulation.SimEvent("BacklogReady")
        Simulation.Process.__init__(self, name='BackpressureScheduler')

    def run(self):
        while(1):
            yield Simulation.hold, self,
            self.client.backlogMonitor.observe(len(self.backlogQueue))
            if (len(self.backlogQueue) != 0):
                task, replicaSet = self.backlogQueue[0]
                sortedReplicaSet = self.client.sort(replicaSet)
                sent = False
                minDurationToWait = 1e10   # arbitrary large value
                minReplica = None
                for replica in sortedReplicaSet:
                    currentTokens = self.client.rateLimiters[replica].tokens
                    self.client.tokenMonitor.observe("%s %s"
                                                     % (replica.id,
                                                        currentTokens))
                    durationToWait = \
                        self.client.rateLimiters[replica].tryAcquire()
                    if (durationToWait == 0):
                        assert self.client.rateLimiters[replica].tokens >= 1
                        self.backlogQueue.pop(0)
                        replicaIndex = sortedReplicaSet.index(replica)
                        if(not replicaIndex == 0):
                            sortedReplicaSet[replicaIndex], sortedReplicaSet[0] =\
                            sortedReplicaSet[0], sortedReplicaSet[replicaIndex]
                        task.addReplicaSet(sortedReplicaSet)
                        if (len(self.client.expectedDelayMap[replica]) != 0):
                            queueSizeEst = self.client.pendingRequestsMap[replica] \
                                    * constants.NUMBER_OF_CLIENTS \
                                    + self.client.expectedDelayMap[replica]["queueSizeAfter"]
                        else:
                            queueSizeEst = 0
                        task.addQueueSizeEst(queueSizeEst)
                        print self.client.id, 'is sending request', task.id, 'to', replica
                        self.client.sendRequest(task, replica, False)
                        self.client.maybeSendShadowReads(replica, replicaSet)
                        sent = True
                        self.client.rateLimiters[replica].update()
                        break
                    else:
                        if durationToWait < minDurationToWait:
                            minDurationToWait = durationToWait
                            minReplica = replica
                        assert self.client.rateLimiters[replica].tokens < 1

                if (not sent):
                    # Backpressure mode. Wait for the least amount of time
                    # necessary until at least one rate limiter is expected
                    # to be available
                    yield Simulation.hold, self, minDurationToWait
                    # NOTE: In principle, these 2 lines below would not be
                    # necessary because the rate limiter would have exactly 1
                    # token after minDurationWait. However, due to
                    # floating-point arithmetic precision we might not have 1
                    # token and this would cause the simulation to enter an
                    # almost infinite loop. These 2 lines by-pass this problem.
                    self.client.rateLimiters[minReplica].tokens = 1
                    self.client.rateLimiters[minReplica].lastSent = Simulation.now()
                    minReplica = None
            else:
                yield Simulation.waitevent, self, self.backlogReadyEvent
                self.backlogReadyEvent = Simulation.SimEvent("BacklogReady")

    def enqueue(self, task, replicaSet):
        self.backlogQueue.append((task, replicaSet))
        self.backlogReadyEvent.signal()


class RateLimiter():
    def __init__(self, id_, client, maxTokens, rateInterval):
        self.id = id_
        self.rate = 5
        self.lastSent = 0
        self.client = client
        self.tokens = maxTokens
        self.rateInterval = rateInterval
        self.maxTokens = maxTokens

    # These updates can be forced due to shadowReads
    def update(self):
        self.lastSent = Simulation.now()
        self.tokens -= 1

    def tryAcquire(self):
        tokens = min(self.maxTokens, self.tokens
                     + self.rate/float(self.rateInterval)
                     * (Simulation.now() - self.lastSent))
        if (tokens >= 1):
            self.tokens = tokens
            return 0
        else:
            assert self.tokens < 1
            timetowait = (1 - tokens) * self.rateInterval/self.rate
            return timetowait

    def forceUpdates(self):
        self.tokens -= 1

    def getTokens(self):
        return min(self.maxTokens, self.tokens
                   + self.rate/float(self.rateInterval)
                   * (Simulation.now() - self.lastSent))


class ReceiveRate():
    def __init__(self, id, interval):
        self.rate = 10
        self.id = id
        self.interval = int(interval)
        self.last = 0
        self.count = 0

    def getRate(self):
        self.add(0)
        return self.rate

    def add(self, requests):
        now = int(Simulation.now()/self.interval)
        if (now - self.last < self.interval):
            self.count += requests
            if (now > self.last):
                # alpha = (now - self.last)/float(self.interval)
                alpha = 0.9
                self.rate = alpha * self.count + (1 - alpha) * self.rate
                self.last = now
                self.count = 0
        else:
            self.rate = self.count
            self.last = now
            self.count = 0


class DynamicSnitch(Simulation.Process):
    '''
    Model for Cassandra's native dynamic snitching approach
    '''
    def __init__(self, client, snitchUpdateInterval):
        self.SNITCHING_INTERVAL = snitchUpdateInterval
        self.client = client
        Simulation.Process.__init__(self, name='DynamicSnitch')

    def run(self):
        # Start each process with a minor delay

        while(1):
            yield Simulation.hold, self, self.SNITCHING_INTERVAL

            # Adaptation of DynamicEndpointSnitch algorithm
            maxLatency = 1.0
            maxPenalty = 1.0
            latencies = [entry.get_snapshot().get_median()
                         for entry in self.client.latencyEdma.values()]
            latenciesGtOne = [latency for latency in latencies if latency > 1.0]
            if (len(latencies) == 0):  # nothing to see here
                continue
            maxLatency = max(latenciesGtOne) if len(latenciesGtOne) > 0 else 1.0
            penalties = {}
            for peer in self.client.serverList:
                penalties[peer] = self.client.lastSeen[peer]
                penalties[peer] = Simulation.now() - penalties[peer]
                if (penalties[peer] > self.SNITCHING_INTERVAL):
                    penalties[peer] = self.SNITCHING_INTERVAL

            penaltiesGtOne = [penalty for penalty in penalties.values()
                              if penalty > 1.0]
            maxPenalty = max(penalties.values()) \
                if len(penaltiesGtOne) > 0 else 1.0

            for peer in self.client.latencyEdma:
                score = self.client.latencyEdma[peer] \
                            .get_snapshot() \
                            .get_median() / float(maxLatency)

                if (peer in penalties):
                    score += penalties[peer] / float(maxPenalty)
                else:
                    score += 1
                assert score >= 0 and score <= 2.0
                self.client.dsScores[peer] = score
