import constants
from collections import defaultdict

class Path():
    def __init__(self, path_elements=False):
        if(path_elements):
            self.path_elements = path_elements
        else:
            self.path_elements = list()
        self.path_key = hash(str(self.path_elements))

    def append(self, node):
        self.path_elements.append(node)
        self.path_key = hash(str(self.path_elements))

    def prepend(self, node):
        self.path_elements.insert(0, node)
        self.path_key = hash(str(self.path_elements))

    def getNextHop(self, currentNode):
        i = self.path_elements.index(currentNode)
        return self.path_elements[i+1]

    def getLastNode(self):
        return self.path_elements[len(self.path_elements)-1]

    def getFirstNode(self):
        return self.path_elements[0]

    def removeLastNode(self):
        self.path_elements.remove(self.getLastNode())

    def removeFirstNode(self):
        self.path_elements.remove(self.getFirstNode())

    def reverse(self):
        self.path_elements.reverse()
        self.path_key = hash(str(self.path_elements))

    def clone(self):
        return Path(list(self.path_elements))

    def getLength(self):
        return len(self.path_elements)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.path_key == other.path_key
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.path_key

    def __str__(self):
        return str(self.path_elements)

    def __repr__(self):
        return self.__str__()

class PathLookupTable():
    def __init__(self, switch):
        self.fwd_path_latencies = defaultdict(dict)
        self.bck_path_latencies = defaultdict(dict)
        self.history = {} #routing history of requests (necessary to maintain same route for packets that fulfill the same request)
        self.switch = switch
        self.PerPacketMultiPath = constants.PER_PACKET_FORWARDING # whether packets are forwarded independently of each other

    def addPath(self, path):
        #print path, self.switch
        if(path.getFirstNode() == self.switch):
            self.fwd_path_latencies[path.getLastNode()][path] = 0
            #print self.fwd_path_latencies
        else:
            self.bck_path_latencies[path.getFirstNode()][path] = 0

    def updatePath(self, latency, path):
        if(path.getFirstNode() == self.switch):
            table = self.fwd_path_latencies
            key = path.getLastNode()
        else:
            table = self.bck_path_latencies
            key = path.getFirstNode()
        if path in table[path.getLastNode()]:
            table[key][path] = self.ewma(table[path.getLastNode()][path], latency)
        else:
            table[key][path] = latency

    def updatePathForTask(self, latency, task):
        path = self.history[task.id][0]
        if path in self.fwd_path_latencies[path.getLastNode()]:
            self.fwd_path_latencies[path.getLastNode()][path] = self.ewma(self.fwd_path_latencies[path.getLastNode()][path], latency)
        else:
            self.fwd_path_latencies[path.getLastNode()][path] = latency

        assert(task.id in self.history)

        #if packet isn't cut, update history
        if(not task.isCut):
            self.history[task.id] = (self.history[task.id][0], self.history[task.id][1]+1)
            if self.history[task.id][1] >= task.requestPktCount:
                #if all responses received, delete history
                #FIXME what if the packet gets dropped in the last hop?
                self.history.pop(task.id)


    #Get shortest path to destination client/server
    def getShortestPath(self, task, forward=True, history=True):
        if(task.id in self.history and history):
            return self.history[task.id][0]

        if(forward):
            table = self.fwd_path_latencies
        else:
            table = self.bck_path_latencies

        shortestPath = (min(table[task.dst], key=table[task.dst].get))

        #save path if forward
        if(not self.PerPacketMultiPath and forward and history):
            self.history[task.id] = (shortestPath, 0)
        return shortestPath

    def getShortestPathToLeaf(self, task, forward=True, history=True):
        #print task.src, task.dst, self.switch, task.dst.getUppers()[0]
        #print self.fwd_path_latencies
        if(task.id in self.history and history):
            return self.history[task.id][0]

        if(forward):
            table = self.fwd_path_latencies
        else:
            table = self.bck_path_latencies

        dstLeafSW = task.dst.getUppers()[0]

        shortestPath = min(table[dstLeafSW], key=table[dstLeafSW].get)
        #save path
        if(not self.PerPacketMultiPath and history):
            self.history[task.id] = (shortestPath, 0)
        return shortestPath

    def getShortestPath_oracle(self, task, forward=True, history=True):
        if(task.id in self.history and history):
            return self.history[task.id][0]
        if(forward):
            table = self.fwd_path_latencies
        else:
            table = self.bck_path_latencies
        shortestPath = min(table[task.dst].keys(), key=self.getLatency_oracle)

        #save path if forward
        if(not self.PerPacketMultiPath and forward and history):
            self.history[task.id] = (shortestPath, 0)
        return shortestPath

    def getShortestPathToLeaf_oracle(self, task, forward=True, history=True):
        if(task.id in self.history and history):
            return self.history[task.id][0]
        if(forward):
            table = self.fwd_path_latencies
        else:
            table = self.bck_path_latencies

        dstLeafSW = task.dst.getUppers()[0]

        shortestPath = min(table[dstLeafSW].keys(), key=self.getLatency_oracle)

        #save path if forward
        if(not self.PerPacketMultiPath and forward and history):
            self.history[task.id] = (shortestPath, 0)
        return shortestPath

    @staticmethod
    def getLatency_oracle(path):
        #FIXME pass actual packet size
        n = path.getFirstNode()
        waitingTime = 0
        for i in range(path.getLength()-1):
            p = n.getPort(path.getNextHop(n))
            waitingTime += (len(p.buffer.waitQ)+1)*p.getTxTime_size(constants.PACKET_SIZE)
            n = path.getNextHop(n)
        return waitingTime


    def getRateLimitedShortestPath(self, task):
        #FIXME various issues; needs an overhaul
        alpha = 0.8
        dstLeafSW = task.dst.getUppers()[0]
        minPath = None
        minTT = 1e9
        for path in self.path_latencies[dstLeafSW].keys():
            #2nd-level rate limiter eq: (qi+ba)/ri
            p = self.switch.getPort(path.getNextHop(self.switch))
            num = task.requestPktCount * (constants.PACKET_SIZE*1e6) + p.getQueueSize()*constants.PACKET_SIZE #simplifying assumption that all packets have the same size
            denom = (1- (alpha * self.path_latencies[dstLeafSW][path] - constants.TARGET_LATENCY)/float(constants.TARGET_LATENCY))
            expTT = num/float(denom)
            if(expTT < minTT):
                minTT = expTT
                minPath = path
        #save path
        if(not self.PerPacketMultiPath):
            self.history[task.id] = (minPath, 0)
        return minPath



    #latency defaults to zero if path is non-existent
    #FIXME should we decrement stale path latencies?
    def get(self, path):
        if path.getFirstNode() == self.switch:
            return self.fwd_path_latencies[path.getLastNode()][path]
        else:
            return self.bck_path_latencies[path.getFirstNode()][path]

    @staticmethod
    def ewma(old, new):
        alpha = 0.9
        return alpha * new + (1 - alpha) * old
