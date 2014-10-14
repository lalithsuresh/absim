import task

class DataTask(task.Task):
    """This is a data task which accounts for a task's size in bytes"""
    def __init__(self, id_, latencyMonitor, src=None, dst=None, size=1, response=False, seqN=1, total=1):
        task.Task.__init__(self, id_, latencyMonitor)
        self.response = response
        self.size = size
        self.src = src
        self.dst = dst
        self.seqN = seqN
        self.total = total
        
    def setDestination(self, dst):
        self.dst = dst
        
    def incSeq(self):
        self.seqN = self.seqN + 1