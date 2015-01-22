"creat by Jing Li"

import numpy
from numpy.random.mtrand import poisson
import simpy
import sys
from sequential import Sequential
from parallel import Parallel


class Stage(object):
    def __init__(self, env, id_, serverlist, replicaSelectionStrategy, composition=None, timeout=None,
                 ifRequestStage=False):
        self.env = env
        self.id_ = id_
        self.serverlist = serverlist
        self.replicaSelectionStrategy = replicaSelectionStrategy
        self.composition = composition
        self.timeout = timeout
        self.ifRequestStage = ifRequestStage
        # self.requestsucceed = False
        # self.ctrl_reissue = env.event()
        # self.ctrl_succeed = env.event()
        # self.ifReissue = ifReissue
        # self.action = env.process(self.execute())

    def resetRequestState(self):
        self.requestsucceed = False

    def getReplica(self):
        sortedReplicaSet = self.sort(self.serverlist)
        return sortedReplicaSet[0]

    def sort(self, originalReplicaSet):
        replicaSet = originalReplicaSet[0:]

        if (self.replicaSelectionStrategy == "random"):
            numpy.random.shuffle(replicaSet)
        else:
            print self.replicaSelectionStrategy
            assert False, "REPLICA_SELECTION_STRATEGY isn't set or is invalid"
        return replicaSet

    def reissue_ctrl(self):
        # if self.timeout is not None:
        reissue = None
        try:
            yield self.env.timeout(self.timeout)
            # self.ctrl_reissue.succeed()
            # self.ctrl_reissue = env.event()
            reissue = self.env.process(self.composition.execute())
            yield reissue
            # print("reissue finished first.")
        except simpy.Interrupt as i:
            pass
            # print('at ', env.now, i.cause)
            # # if reissue is not None:
            # #     if not reissue.processed:
            # #         reissue.interrupt('interrupt')


    def execute(self, start=0, requestcounter=0):

        # try:

        if self.composition is not None:
            wait_composition = self.env.process(self.composition.execute())
            if self.timeout is not None:
                reissue_ctrl = self.env.process(self.reissue_ctrl())
                yield wait_composition | reissue_ctrl
                if not wait_composition.processed:
                    # reissue = self.env.process(self.execute())
                    wait_composition.interrupt("interrupt")
                if not reissue_ctrl.triggered:
                    reissue_ctrl.interrupt("reissue interrupt")
            else:
                yield wait_composition
        # print('Server "%s" run at %d' % (self.id_, env.now))
        replicaToServe = self.getReplica()
        servT = replicaToServe.getServiceTime()
        yield self.env.timeout(servT)
        end = env.now
        # print('Server "%s" finish at %d' % (self.id_, env.now))

        # if self.ifRequestStage and not self.requestsucceed:
        if self.ifRequestStage:
            response = end - start
            print('%d %d' % (requestcounter, response))
            # print('%d' % response)
            # self.requestsucceed = True

            # except simpy.Interrupt as i:
            # print(self.id_, 'interrupted at', env.now, 'msg:', i.cause)
            #     print('Stage', self.id_, 'terminated itself after all its dependency Stages at ', env.now)


class Server():
    def __init__(self, env, id_, serviceTime, serviceTimeModel):
        self.env = env
        self.id_ = id_
        self.serviceTime = serviceTime
        self.serviceTimeModel = serviceTimeModel
        self.waitTime = 0

    def getServiceTime(self):
        serviceTime = 0.0
        if (self.serviceTimeModel == "random.expovariate"):
            serviceTime = numpy.random.expovariate(1.0 / (self.serviceTime))
        elif (self.serviceTimeModel == "constant"):
            serviceTime = self.serviceTime
        else:
            print "Unknown service time model"
            sys.exit(-1)
        return serviceTime

    def getWaitTime(self):
        return self.waitTime

        # def run(self, composition):
        # servT = self.getServiceTime()  # Mu_i
        #     try:
        #         if composition is not None:
        #             yield self.env.process(composition.execute())
        #         # print('Server "%s" run at %d' % (self.id_, env.now))
        #         yield self.env.timeout(servT)
        #         # print('Server "%s" finish at %d' % (self.id_, env.now))
        #     except simpy.Interrupt as i:
        #         pass
        #         # print(self.id_, 'interrupted at', env.now, 'msg:', i.cause)
        #
        #         # ctrl_succeed.succeed()
        #         # # ctrl_succeed = env.event()


def worload(env, model, model_param, stage, numRequests):
    requestCounter = 0
    while (numRequests != 0):
        requestCounter += 1
        stage.resetRequestState()
        start = env.now
        # print('the %d request start at %d' % (requestCounter, start))
        env.process(stage.execute(start, requestCounter))
        if (model == "poisson"):
            yield env.timeout(poisson(model_param))
        if (model == "constant"):
            yield env.timeout(model_param)
        numRequests -= 1


env = simpy.Environment()

#constants.SERVER_MODEL = "random.expovariate"

server1_a = Server(env, "server1_a", 5, "constant")
server1_b = Server(env, "server1_b", 10, "constant")
server1_c = Server(env, "server1_c", 1, "constant")

server2_a = Server(env, "server2_a", 20, "constant")
server2_b = Server(env, "server2_b", 5, "constant")
server2_c = Server(env, "server2_c", 4, "constant")
server2_d = Server(env, "server2_d", 10, "constant")

server3_a = Server(env, "server3_a", 6, "constant")
server3_b = Server(env, "server3_b", 30, "constant")
server3_c = Server(env, "server3_c", 9, "constant")
server3_d = Server(env, "server3_d", 3, "constant")

server4_a = Server(env, "server4_a", 6, "constant")
server4_b = Server(env, "server4_b", 8, "constant")

s4 = Stage(env, 4, [server4_a, server4_b], "random")
s3 = Stage(env, 3, [server3_a, server3_b, server3_c, server3_d], "random")
# s2 = Stage(env, 2, [server2_a, server2_b, server2_c, server2_d], "random", Sequential(env, s3))
s2 = Stage(env, 2, [server2_a, server2_b, server2_c, server2_d], "random", Sequential(env, s3), 4)
# s1 = Stage(env, 1, [server1_a, server1_b, server1_c], "random", Sequential(env, s2), ifRequestStage=True)
s1 = Stage(env, 1, [server1_a, server1_b, server1_c], "random", Sequential(env, s2), 11, ifRequestStage=True)

# s1 = Stage(env, 1, [server1], "random", Sequential(env, s2, s3))
# s1 = Stage(env, 1, [server1], "random", Sequential(env, s2, s3), ifRequestStage=True)
# s1 = Stage(env, 1, [server1], "random", Sequential(env, s3, s2), 8, ifRequestStage=True)

# model = "poisson"
# model_param = 1
# numRequests = 1000

env.process(worload(env, model="poisson", model_param=2, stage=s1, numRequests=1000))
# env.process(worload(env, model="poisson", model_param=2, stage=s2, numRequests=1000))
# env.process(worload(env, model="poisson", model_param=2, stage=s3, numRequests=1000))
# s_parallel = Stage(env, 1, [server1], "random", Parallel(env, s2, s3))
# env.process(s_parallel.execute())
# s_more = Stage(env, 1, [server1], "random", Sequential(env, s2, Parallel(env, s3, s4)), 4)
# env.process(s_more.execute())
# env.process(s_more.reissue_ctrl())
env.run()

