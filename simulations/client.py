import SimPy.Simulation as Simulation
import random
import numpy
import constants
import task
from misc import DeliverMessageWithDelay
from node import Node

class Client(Node):
    def __init__(self, id_, serverList, replicaSelectionStrategy,
                 accessPattern, replicationFactor, backpressure,
                 shadowReadRatio):
        Node.__init__(self, id_, "client")
        self.serverList = serverList
        self.accessPattern = accessPattern
        self.replicationFactor = replicationFactor
        self.REPLICA_SELECTION_STRATEGY = replicaSelectionStrategy
        self.pendingRequestsMonitor = Simulation.Monitor(name="PendingRequests")
        self.latencyTrackerMonitor = Simulation.Monitor(name="LatencyTracker")
        self.movingAverageWindow = 10
        self.backpressure = backpressure    # True/Flase
        self.shadowReadRatio = shadowReadRatio
        self.requestStatus = {} #request/response mappings to keep track of response status
        self.sentRequests = []
        # Book-keeping and metrics to be recorded follow...

        # Number of outstanding requests at the client
        self.pendingRequestsMap = {node: 0 for node in self.serverList}
        #print 'number of servers', len(self.serverList)
        #print 'pending requests', self.pendingRequestsMap
        # Number of outstanding requests times oracle-service time of replica
        self.pendingXserviceMap = {node: 0 for node in self.serverList}
        # Last-received response time of server
        self.responseTimesMap = {node: 0 for node in self.serverList}

        # Used to track response time from the perspective of the client
        self.taskSentTimeTracker = {}
        self.taskArrivalTimeTracker = {}

        # Record waiting and service times as relayed by the server
        self.expectedDelayMap = {node: [] for node in self.serverList}

        # Queue size thresholds per replica
        self.queueSizeThresholds = {node: 1 for node in self.serverList}

        # Backpressure related initialization
        self.backpressureSchedulers =\
            {node: BackpressureScheduler("BP%s" % (node.id), self)
                for node in serverList}

        self.muMax = 0.0

        for node, sched in self.backpressureSchedulers.items():
            Simulation.activate(sched, sched.run(), at=Simulation.now())

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
        self.taskArrivalTimeTracker[task.id] = startTime
        receiptTracker = ReceiptTracker()
        Simulation.activate(receiptTracker,
                            receiptTracker.run(self, task, replicaToServe),
                            at=Simulation.now())
        if(self.backpressure is False):
            sortedReplicaSet = self.sort(replicaSet)
            replicaToServe = sortedReplicaSet[0]
            task.replicas = replicaSet
            task.setDestination(replicaToServe)
            self.sendRequest(task, replicaToServe, False)
            self.maybeSendShadowReads(replicaToServe, replicaSet)
        else:
            firstReplica = self.serverList[firstReplicaIndex]
            self.backpressureSchedulers[firstReplica].enqueue(task, replicaSet)

    def sendRequest(self, task, replicaToServe, retransmit):
        # Get switch I'm delivering to
        #print 'neighbors', self.getNeighbors(), 'for client', self.id
        nextSwitch = self.getNeighbors().keys()[0]
        # Get port I'm delivering through
        egress = self.getPort(nextSwitch)
        #print 'client', self.id, 'sending to switch:', nextSwitch.id, 'dst:', task.dst.id
        if(retransmit):
            task.receivedEvent = Simulation.SimEvent("PacketReceipt")
        egress.enqueueTask(task)
        

        #print 'server list', self.serverList
        #print 'pending requests', self.pendingRequestsMap
        # Book-keeping for metrics
        self.pendingRequestsMap[replicaToServe] += 1
        self.pendingXserviceMap[replicaToServe] = \
            (1 + self.pendingRequestsMap[replicaToServe]) \
            * replicaToServe.serviceTime
        self.pendingRequestsMonitor.observe(
            "%s %s" % (replicaToServe.id,
                       self.pendingRequestsMap[replicaToServe]))
        if not retransmit:
            self.taskSentTimeTracker[task.id] = Simulation.now()
        self.sentRequests.append(task)

    def receiveResponse(self, packet):
        packet.sigTaskReceived(False)
        #print 'Client received response with ID:', packet.id
        if packet.id in self.requestStatus:
            status = self.requestStatus[packet.id]
            status.remove(packet.seqN)
            if(len(status)==0):
                #Response received in full
                #print self.id, 'successfully received packet'
                self.updateStats(packet, packet.src)
        else:
            if packet.count <= 1:
                #Response received in full
                #print self.id, 'successfully received packet'
                self.updateStats(packet, packet.src)
            else:
                required_pieces = [i for i in xrange(1, packet.count+1)]
                required_pieces.remove(packet.seqN)
                self.requestStatus[packet.id] = required_pieces
                
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
                - (entry["serviceTime"] + entry["waitTime"])
            total += (twiceNetworkLatency +
                      (1 + entry["queueSizeAfter"]
                      + self.pendingRequestsMap[replica])
                      * entry["serviceTime"])
        return total

    def maybeSendShadowReads(self, replicaToServe, replicaSet):
        if (random.uniform(0, 1.0) < self.shadowReadRatio):
            for replica in replicaSet:
                if (replica is not replicaToServe):
                    shadowReadTask = task.Task("ShadowRead", None)
                    self.taskTimeSentTracker[shadowReadTask] = Simulation.now()
                    self.sendRequest(shadowReadTask, replica)

    def updateStats(self, task, replicaToServe):

        # OMG request completed. Time for some book-keeping
        self.pendingRequestsMap[replicaToServe] -= 1
        self.pendingXserviceMap[replicaToServe] = \
            (1 + self.pendingRequestsMap[replicaToServe]) \
            * replicaToServe.serviceTime

        self.pendingRequestsMonitor.observe(
            "%s %s" % (replicaToServe.id,
                       self.pendingRequestsMap[replicaToServe]))

        self.responseTimesMap[replicaToServe] = \
            Simulation.now() - self.taskSentTimeTracker[task.id]
        self.latencyTrackerMonitor\
              .observe("%s %s" % (replicaToServe.id,
                       Simulation.now() - self.taskSentTimeTracker[task.id]))
        expDelayMap = task.completionEvent.signalparam
        expDelayMap["responseTime"] = self.responseTimesMap[replicaToServe]
        self.expectedDelayMap[replicaToServe]\
            .append(expDelayMap)

        # TODO: Threshold
        if (len(self.expectedDelayMap[replicaToServe])
           > self.movingAverageWindow):
            self.expectedDelayMap[replicaToServe].pop(0)

        # Backpressure related book-keeping
        if (self.backpressure):
            mus = []
            for replica in self.serverList:
                mus.append(sum([entry.get("serviceTime")
                                for entry in self.expectedDelayMap[replica]]))
            self.muMax = max(mus)

            shuffledNodeList = self.serverList[0:]
            random.shuffle(shuffledNodeList)
            for node in shuffledNodeList:
                self.backpressureSchedulers[node].congestionEvent.signal()

            expDelay = self.computeExpectedDelay(replicaToServe)
            if (self.muMax > expDelay):
                self.queueSizeThresholds[replicaToServe] += 1
            elif (self.muMax < expDelay):
                self.queueSizeThresholds[replicaToServe] /= 2

            if (self.queueSizeThresholds[replicaToServe] < 1
               and self.pendingRequestsMap[replicaToServe] == 0):
                self.queueSizeThresholds[replicaToServe] = 1

        del self.taskSentTimeTracker[task.id]
        del self.taskArrivalTimeTracker[task.id]
        del self.requestStatus[task.id]
        # Does not make sense to record shadow read latencies
        # as a latency measurement
        if (task.latencyMonitor is not None):
            #print 'Task ID:', task.id
            #print 'Start Time:', task.start, ', Sim. Time:', Simulation.now()
            task.latencyMonitor.observe(Simulation.now() - task.start)


class ReceiptTracker(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name="ReceiptTracker")
        
    def run(self, client, task, replicaToServe):
        yield Simulation.hold, self,
        yield Simulation.waitevent, self, task.receivedEvent
        client.sentRequests.remove(task)
        
        if(task.receivedEvent.signalparam): #This means that the packet has been dropped
            client.pendingRequestsMap[replicaToServe] -= 1
            client.pendingXserviceMap[replicaToServe] = \
                (1 + client.pendingRequestsMap[replicaToServe]) \
                * replicaToServe.serviceTime
            newReplica = client.sort(task.replicas)[0]
            client.sendRequest(task, newReplica, True)
            #print 'Client is resending dropped packet with ID:', task.id
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
                replicaToServe = sortedReplicaSet[0]

                if (self.client.queueSizeThresholds[replicaToServe] >
                   self.client.pendingRequestsMap[replicaToServe]):
                    self.backlogQueue.pop(0)
                    self.client.sendRequest(task, replicaToServe)
                    self.client.maybeSendShadowReads(replicaToServe, replicaSet)
                else:
                    # Enter congestion state
                    # print self.id, 'conge'
                    yield Simulation.waitevent, self, self.congestionEvent
                    # print self.id, 'deconge'
                    self.congestionEvent = Simulation.SimEvent("Congestion")
                    continue
            else:
                yield Simulation.waitevent, self, self.backlogReadyEvent
                self.backlogReadyEvent = Simulation.SimEvent("BacklogReady")

    def enqueue(self, task, replicaSet):
        self.backlogQueue.append((task, replicaSet))
        self.id, "enqueue", len(self.backlogQueue), Simulation.now()
        self.backlogReadyEvent.signal()
