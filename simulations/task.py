import SimPy.Simulation as Simulation


class Task():
    """A simple Task. Applications may subclass this
       for holding specific attributes if need be"""
    def __init__(self, id_, latencyMonitor, start=False, replicaSet=[], queueSizeEst=0):
        self.id = id_
        if not start:
            self.start = Simulation.now()
        else:
            self.start = start
        self.completionEvent = Simulation.SimEvent("ClientToServerCompletion")
        self.latencyMonitor = latencyMonitor
        self.replicaSet = replicaSet
        self.queueSizeEst = queueSizeEst
        
    # Used as a notifier mechanism
    def sigTaskComplete(self, piggyBack=None):
        if (self.completionEvent is not None):
            self.completionEvent.signal(piggyBack)
            
    def addReplicaSet(self, replicaSet):
        self.replicaSet = replicaSet
        
    def addQueueSizeEst(self, queueSizeEst):
        self.queueSizeEst = queueSizeEst
        
    def __str__(self):
        return self.id
    
    def __repr__(self):
        return self.id
    
    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id