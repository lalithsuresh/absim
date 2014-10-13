import task

class DataTask(task.Task):
    """This is a data task which accounts for a task's size in bytes"""
    def __init__(self, replica_server, response=False, size):
        self.replica_server
        self.response = response
        self.size = size