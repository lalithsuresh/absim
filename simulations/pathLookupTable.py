from collections import defaultdict

class Path():
    def __init__(self):
        self.path_elements = list()
        self.path_key = ""

    def add(self, node):
        self.path_elements.append(node)
        self.path_key = str(self.path_elements)

    def nextHop(self, currentNode):
        i = self.path_elements.index(currentNode)
        return self.path_elements[i+1]

    def getLastElem(self):
        return self.path_elements[len(self.path_elements)-1]

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
    def __init__(self):
        self.path_latencies = defaultdict(dict)

    def update(self, path, latency):
        if path in self.path_latencies[path.getLastElem()]:
            self.path_latencies[path.getLastElem()][path] = self.ewma(self.path_latencies[path], latency)
        else:
            self.path_latencies[path.getLastElem()][path] = latency

    #Get shortest path to destination client/server
    def getShortestPath(self, dst):
        dstLeafSW = dst.getUppers()
        shortestPath = min(self.path_latencies[dstLeafSW], key=self.path_latencies[dstLeafSW].get)
        return shortestPath

    #latency defaults to zero if path is non-existent
    #FIXME should we decrement stale path latencies?
    def get(self, path):
        return self.path_latencies.get(path, 0)

    @staticmethod
    def ewma(self, old, new):
        alpha = 0.9
        return alpha * new + (1 - alpha) * old