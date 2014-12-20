"Create by Jing Li"


import simpy\


class Sequential():

    def __init__(self, env, lop, rop):
        self.env = env
        self.lop = lop
        self.rop = rop


    def excute(self):
        yield self.env.process(self.lop.excute())
        yield self.env.process(self.rop.excute())


        # self.lop.excute()
        # wait(event_lop)
        # self.rop.excute()
        # wait(event_rop)
        #
        # wait_on_both(event_lop, event_rop)
        #
        # if isinstance(self.lop, Stage):
        #     ls = self.lop.getReplica()
        #     lw = ls.getServiceTime()
        #     yield Simulation.hold, self, lw
        #     if isinstance(self.rop, Stage):
        #         rs = self.rop.getReplica()
        #         rw = rs.getServiceTime()
        #         yield Simulation.hold, self, rw
        #         self.wait = lw + rw
        #     else:
        #         self.rop.excute()
        #         self.wait = lw + self.rop.getWait()
        # else:
        #     self.lop.excute()
        #     if isinstance(self.rop, Stage):
        #         rs = self.rop.getReplica()
        #         rw = rs.getServiceTime()
        #         yield Simulation.hold, self, rw
        #         self.wait = self.lop.getWait() + rw
        #     else:
        #         self.rop.excute()
        #         self.wait = self.lop.getWait() + self.rop.getWait()