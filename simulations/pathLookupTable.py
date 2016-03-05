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

class PathLookupTable():
    def __init__(self, switch):
        self.path_latencies = defaultdict(dict)
        self.history = {} #routing history of requests (necessary to maintain same route for packets that fulfill the same request)
        self.switch = switch
        self.PerPacketMultiPath = False # packets are forwarded independently of each other

    def addPath(self, path):
        self.path_latencies[path.getLastNode()][path] = 0

    def updatePath(self, latency, path):
        if path in self.path_latencies[path.getLastNode()]:
            self.path_latencies[path.getLastNode()][path] = self.ewma(self.path_latencies[path.getLastNode()][path], latency)
        else:
            self.path_latencies[path.getLastNode()][path] = latency

    def updatePathForTask(self, latency, task):
        path = self.history[task.id][0]
        if path in self.path_latencies[path.getLastNode()]:
            self.path_latencies[path.getLastNode()][path] = self.ewma(self.path_latencies[path.getLastNode()][path], latency)
        else:
            self.path_latencies[path.getLastNode()][path] = latency

        assert(task.id in self.history)

        #if packet isn't cut, update history
        if(not task.isCut):
            self.history[task.id] = (self.history[task.id][0], self.history[task.id][1]+1)
            if self.history[task.id][1] >= task.count:
                #if all responses received, delete history
                #FIXME what if the packet gets dropped in the last hop?
                self.history.pop(task.id)


    #Get shortest path to destination client/server
    def getShortestPath(self, task):
        dstLeafSW = task.dst.getUppers()[0]
        shortestPath = min(self.path_latencies[dstLeafSW], key=self.path_latencies[dstLeafSW].get)
        #save path
        if(not self.PerPacketMultiPath):
            self.history[task.id] = (shortestPath, 0)
        return shortestPath

    def getRateLimitedShortestPath(self, task):
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
        return self.path_latencies.get(path, 0)

    @staticmethod
    def ewma(old, new):
        alpha = 0.9
        return alpha * new + (1 - alpha) * old