import constants
from collections import defaultdict

class Path():
    def __init__(self):
        self.path_elements = list()
        self.path_key = ""

    def append(self, node):
        self.path_elements.append(node)
        self.path_key = str(self.path_elements)

    def prepend(self, node):
        self.path_elements.insert(0, node)
        self.path_key = str(self.path_elements)

    def nextHop(self, currentNode):
        i = self.path_elements.index(currentNode)
        return self.path_elements[i+1]

    def getLastNode(self):
        return self.path_elements[len(self.path_elements)-1]

    def getFirstNode(self):
        return self.path_elements[0]

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.path_key == other.path_key
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.path_key

class PathLookupTable():
    def __init__(self, switch):
        self.path_latencies = defaultdict(dict)
        self.history = {} #routing history of requests (necessary to maintain same route for packets that fulfill the same request)
        self.switch = switch
        self.PerPacketMultiPath = False # packets are forwarded independently of each other

    def update(self, path, latency, task):
        if path in self.path_latencies[path.getLastElem()]:
            self.path_latencies[path.getLastNode()][path] = self.ewma(self.path_latencies[path], latency)
        else:
            self.path_latencies[path.getLastNode()][path] = latency

        assert(task.id in self.history)

        #if packet isn't cut, update history
        if(not task.isCut()):
            self.history[task.id][1] += 1
            if self.history[task.id][1] >= task.count:
                #if all responses received, delete history
                #FIXME what if the packet gets dropped in the last hop?
                self.history.pop(task.id)


    #Get shortest path to destination client/server
    def getShortestPath(self, task, dst):
        dstLeafSW = dst.getUppers()
        shortestPath = min(self.path_latencies[dstLeafSW], key=self.path_latencies[dstLeafSW].get)
        #save path
        if(not self.PerPacketMultiPath):
            self.history[task.id] = (shortestPath, 0)
        return shortestPath

    def getRateLimitedShortestPath(self, task, dst):
        alpha = 0.8
        dstLeafSW = dst.getUppers()
        minPath = None
        minTT = 1e9
        for path in self.path_latencies[dstLeafSW].keys():
            #(qi+bA)/ri , i=1, 2 …, n.
            p = self.switch.getPort(path.getFirstNode())
            num = task.totalSize + p.getQueueSize()*constants.PACKET_SIZE #simplifying assumption that all packets have the same size
            denom = (1- (alpha * self.path_latencies[dstLeafSW][path] - constants.TARGET_LATENCY)/float(constants.TARGET_LATENCY))
            expTT = num/float(denom)
            if(expTT < minTT):
                minTT = expTT
                minPath = path
        #save path
        if(not self.PerPacketMultiPath):
            self.history[task.id] = (path, 0)
        return path



    #latency defaults to zero if path is non-existent
    #FIXME should we decrement stale path latencies?
    def get(self, path):
        return self.path_latencies.get(path, 0)

    @staticmethod
    def ewma(self, old, new):
        alpha = 0.9
        return alpha * new + (1 - alpha) * old