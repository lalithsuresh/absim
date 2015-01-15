import task
import SimPy.Simulation as Simulation
import constants

class DataTask(task.Task):
    """This is a data task which accounts for a task's size in bytes"""
    def __init__(self, id_, latencyMonitor, count=1, src=None, dst=None, size=constants.PACKET_SIZE,\
                  response=False, seqN=0, start=False,\
                   completionEvent=False, receivedEvent=False, replicas=[], queueSizeEst=0):
        task.Task.__init__(self, id_, latencyMonitor, start, replicas, queueSizeEst)
        self.response = response
        self.size = constants.PACKET_SIZE
        self.src = src
        self.dst = dst
        self.seqN = seqN
        self.count = count
        #self.replicas = replicas
        self.requestTask = None
        self.isCut = False    
        #CONGA parameters
        self.ce = 0
        self.lbTag = None
        self.fbMetric = 0
        self.fb = None
        #Server piggybacked feedback
        self.serverFB = None
        
    def setDestination(self, dst):
        self.dst = dst
    
    def copyCONGAParams(self, another):
        self.ce = another.ce
        self.lbTag = another.lbTag
        self.fbMetric = another.fbMetric
        self.fb = another.fb
        
    def setCE(self, ce, lbTag=False):
        self.ce = ce
        if(lbTag):
            self.lbTag = lbTag
    
    def setFB(self, fbMetric, fb):
        self.fbMetric = fbMetric
        self.fb = fb
        
    def cutPacket(self):
        self.isCut = True
        self.size = self.size*0.0763
        temp = self.src
        self.src = self.dst
        self.dst = temp
        
    def restorePacket(self):
        self.isCut = False
        self.size = self.size*(1/0.0763)
        temp = self.src
        self.src = self.dst
        self.dst = temp
    
    def setServerFB(self, fb):
        self.serverFB = fb
        
    def setRequest(self, task):
        self.requestTask = task
        self.response = True
        
    def incSeq(self):
        self.seqN = self.seqN + 1
        
    def __str__(self):
        return self.id
    
    def __repr__(self):
        return self.id
    
    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id