import simpy
import random
import task
import numpy
import datatask


class Workload():

    def __init__(self, env, id_, numRequests, model, model_param):
        self.env = env
        self.id_ = id_
        self.numRequests =numRequests
        self.model = model
        self.model_param = model_param

    # TODO: also need non-uniform client access
    # Need to pin workload to one stage
    def run(self):

        taskCounter = 0

        while(self.numRequests != 0):

            yield Simulation.hold, self,

            # delay = constants.NW_LATENCY_BASE + \
            #     random.normalvariate(constants.NW_LATENCY_MU,
            #                          constants.NW_LATENCY_SIGMA)
            # yield Simulation.hold, self, delay
            taskToSchedule = datatask.DataTask("Task" + str(taskCounter),
                                       self.latencyMonitor)
            taskCounter += 1
            # Push out a task...
            randomClientIndex = random.randint(0, len(clientList) - 1)
            clientNode = clientList[randomClientIndex]
            #print clientList
            #print 'chosen node:', clientNode.id
            taskToSchedule.src = clientNode
            clientNode.schedule(taskToSchedule)

            # Simulate client delay
            if (model == "poisson"):
                yield Simulation.hold, self, numpy.random.poisson(model_param)

            # If model is gaussian, add gaussian delay
            # If model is constant, add fixed delay
            if (model == "constant"):
                yield Simulation.hold, self, model_param

            numRequests -= 1
