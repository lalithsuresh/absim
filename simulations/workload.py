import SimPy.Simulation as Simulation
import random
import task
import numpy
import constants


class Workload(Simulation.Process):

    def __init__(self, id_, latencyMonitor):
        self.latencyMonitor = latencyMonitor
        Simulation.Process.__init__(self, name='Workload' + str(id_))

    def run(self, clientList, model, poisson_param):

        taskCounter = 0

        while(1):
            yield Simulation.hold, self,
            # Simulate client delay
            if(model == "poisson"):
                yield Simulation.hold, self, numpy.random.poisson(poisson_param)

            start = Simulation.now()
            delay = constants.NW_LATENCY_BASE + \
                random.normalvariate(constants.NW_LATENCY_MU,
                                     constants.NW_LATENCY_SIGMA)
            yield Simulation.hold, self, delay
            taskToSchedule = task.Task("Task" + str(taskCounter), None)
            taskCounter += 1

            # Push out a task...
            randomClientIndex = random.randint(0, len(clientList) - 1)
            client = clientList[randomClientIndex]

            client.schedule(taskToSchedule)

            # Wait until the task we sent out is complete.
            yield Simulation.waitevent, self, taskToSchedule.eventExtra
            self.latencyMonitor.observe(Simulation.now() - start)
