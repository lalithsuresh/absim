import SimPy.Simulation as Simulation
from node import Node
from misc import DeliverMessageWithDelay
from misc import cloneDataTask

class Switch(Node):
    """A representation of a physical switch that disperses requests and responses
    Note: We're assuming that switches aren't bottle-necked by processing times"""
    def __init__(self, id_, htype,
                 procTime, selectionStrategy="passive"):
        Node.__init__(self, id_, htype)
        self.procTime = procTime
        self.connectedHosts = {} #Equivalent to switch routing table
        self.selectionStrategy = selectionStrategy
    def addConnectedHosts(self, n, nHosts):
        for h in nHosts:         
            if h not in self.connectedHosts:
                self.connectedHosts[h] = [n]
            else:
                self.connectedHosts[h].append(n)

    def getConnectedHosts(self):
        return self.connectedHosts.keys()
          
    def getIntermediary(self, h):
        if h in self.connectedHosts.keys():
            return self.connectedHosts[h]
     
    def enqueueTask(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getServiceTime(self):
        #serviceTime = task.size/self.bw + self.procTime
        return self.procTime
        #return serviceTime
        
        #Dynamic Load-Balaning algorithm (DLB) from OpenFlow based Load Balancing for Fat-Tree Networks with Multipath Support
    def getNextHop(self, dst):
        #check if I'm direct neighbors with dst
        if(self.isNeighbor(dst)):
            egressPort = self.getPort(dst)
        #check if I'm connected to dst through intermediary node
        elif(self.getIntermediary(dst)):
            possible_hops = self.getIntermediary(dst)
            egressPort = min(possible_hops, key=lambda n: self.getPort(n).getQueueSize())
        else: 
            #We're going up!
            possible_hops = self.getUppers()
            egressPort = min(possible_hops, key=lambda n: self.getPort(n).getQueueSize())
        return egressPort
    
    def getHopCount(self, dst):
        print 'DST', dst.id, dst.htype
        hopCount = 0
        nextNode = self
        while True:
            hopCount += 1
            nextPort = nextNode.getNextHop(dst)
            print 'port', nextPort.__class__.__name__
            nextNode = nextPort.dst
            print 'node', nextNode.__class__.__name__
            print nextNode.id, dst.id, nextNode.htype, dst.htype
            if(nextNode.id == dst.id and nextNode.htype == dst.htype):
                print 'returning..'
                return hopCount
    
class Executor(Simulation.Process):
    
    def __init__(self, switch, task):
        self.switch = switch
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        #Make the next hop
        yield Simulation.hold, self, self.switch.procTime
        if(self.switch.selectionStrategy == "hopDelay"):
            self.task.replicaSet.sort(key=lambda x: self.switch.getHopCount(x)*self.switch.getPort(x).getQueueSize())
            self.task.dst = self.task.replicaSet[0]
        egress = self.switch.getNextHop(self.task.dst)
        egress.enqueueTask(self.task)