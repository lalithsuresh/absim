from collections import namedtuple
from datatask import DataTask
import SimPy.Simulation as Simulation
import constants

class DeliverMessageWithDelay(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='DeliverMessageWithDelay')

    def run(self, task, delay, port):
        yield Simulation.hold, self, delay
        port.enqueueTask(task)

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


def cloneDataTask(task):
    newTask = DataTask(task.id, task.latencyMonitor, task.count, task.src, task.dst,\
                        task.size, task.response, task.seqN, task.start, task.replicaSet,\
                       task.queueSizeEst, task.requestPktCount, task.requestType, task.trafficType)
    return newTask
    
def parse_graph(name='default'):
    graph = {}
    g = open('graph/%s'%name,'r')
    for line in g:
        values = line.split()
        src, dst_list = values[0], values[1:]
        graph[src] = dst_list
        return graph

NodePair = namedtuple("NodePair", ["src", "dst"])

def find_all_paths_for_graph(graph, paths={}):
    for k in graph.keys():
        for v in graph.get(k):
            paths[NodePair(src = k, dst = v)] = find_all_paths_for_nodes(graph, k, v, path=[])
    return paths
       
  
def find_all_paths_for_nodes(graph, start, end, path=[]):
        path = path + [start]
        if start == end:
            return [path]
        if not graph.has_key(start):
            return []
        paths = []
        for node in graph[start]:
            if node not in path:
                newpaths = find_all_paths_for_nodes(graph, node, end, path)
                for newpath in newpaths:
                    paths.append(newpath)
        return paths

class BackgroundTrafficGenerator(Simulation.Process):
    def __init__(self, id, src, dst, flowsize):
        self.id = id
        self.src = src
        self.dst = dst
        self.flowsize = flowsize
        self.delay = constants.BACKGROUND_TRAFFIC_DELAY
        Simulation.Process.__init__(self, name='BackgroundTraffic--%s-%s-%s'%(id,src,dst))

    def run(self):
        while(self.flowsize>0 and not constants.END_SIMULATION):
            #print 'dumbPacket generated'
            dumbTask = DataTask("DumbTask-%s-%s"%(self.src, self.dst), None)
            dumbTask.trafficType = constants.BACKGROUND
            dumbTask.src = self.src
            dumbTask.dst = self.dst
            nextSwitch = self.src.getNeighbors().keys()[0]
            # Get port I'm delivering through
            egress = self.src.getPort(nextSwitch)
            # Immediately send out request
            egress.enqueueTask(dumbTask)
            #print 'enqueuing task', egress
            self.flowsize -= 1
            yield Simulation.hold, self, self.delay