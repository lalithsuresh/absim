"creat by Jing Li"

import numpy
import simpy
import sys
from sequential import Sequential
from parallel import Parallel


class Stage(object):
    def __init__(self, env, id_, serverlist, replicaSelectionStrategy, composition=None, timeout=None):
        self.env = env
        self.id_ = id_
        self.serverlist = serverlist
        self.replicaSelectionStrategy = replicaSelectionStrategy
        self.composition = composition
        self.timeout = timeout
        self.ctrl_reissue = env.event()
        self.ctrl_succeed = env.event()
        # self.ifReissue = ifReissue
        # self.action = env.process(self.excute())

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
            global reissue
            reissue = None
            try:
                yield self.env.timeout(self.timeout)
                # self.ctrl_reissue.succeed()
                # self.ctrl_reissue = env.event()
                reissue = self.env.process(self.excute())
                yield reissue
                print("reissue finished first.")
            except simpy.Interrupt as i:
                print('at ', env.now, i.cause)
                if reissue is not None:
                    if not reissue.processed:
                        reissue.interrupt('interrupt')


    def excute(self):
        try:
            replicaToServe = self.getReplica()
            wait_composition = self.env.process(replicaToServe.run(self.composition))
            # reissue_ctrl = self.env.process(self.reissue_ctrl())
            if self.timeout is not None:
                reissue_ctrl = self.env.process(self.reissue_ctrl())
                yield wait_composition | reissue_ctrl
                if not wait_composition.processed:
                    # reissue = self.env.process(self.excute())
                    wait_composition.interrupt("interrupt")
                    # wait_composition.fail(e)
                    print("sent interrupt msg")
                    # yield wait_composition | reissue
                    # if not wait_composition.triggered:
                    #     wait_composition.interrupt("interrupt")
                if not reissue_ctrl.processed:
                    reissue_ctrl.interrupt("reissue interrupt")
            else:
                yield wait_composition
            print('Stage', self.id_, 'run and finished itself after all its dependency Stages at ', env.now)
        except simpy.Interrupt as i:
            print(self.id_, 'interrupted at', env.now, 'msg:', i.cause)
            print('Stage', self.id_, 'terminated itself after all its dependency Stages at ', env.now)



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

    def run(self, composition):
        servT = self.getServiceTime()  # Mu_i
        try:
            if composition is not None:
                yield self.env.process(composition.excute())
            print('Server "%s" run at %d' % (self.id_, env.now))
            yield self.env.timeout(servT)
            print('Server "%s" finish at %d' % (self.id_, env.now))
        except simpy.Interrupt as i:
            print(self.id_, 'interrupted at', env.now, 'msg:', i.cause)

        # ctrl_succeed.succeed()
        # # ctrl_succeed = env.event()

env = simpy.Environment()
server1 = Server(env, "server1", 5, "constant")
server2 = Server(env, "server2", 10, "constant")
server3 = Server(env, "server3", 1, "constant")
server4 = Server(env, "server4", 6, "constant")
s2 = Stage(env, 2, [server2, server3], "random")
s3 = Stage(env, 3, [server3], "random")
s4 = Stage(env, 4, [server4, server3], "random")
s1 = Stage(env, 1, [server1], "random", Sequential(env, s2, s3), 8)

env.process(s1.excute())
env.process(s1.excute())
# s_parallel = Stage(env, 1, [server1], "random", Parallel(env, s2, s3))
# env.process(s_parallel.excute())
# s_more = Stage(env, 1, [server1], "random", Sequential(env, s2, Parallel(env, s3, s4)), 4)
# env.process(s_more.excute())
# env.process(s_more.reissue_ctrl())
env.run(until=20)

