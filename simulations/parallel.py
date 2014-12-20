
# import simpy

__author__ = 'Jing Li'

class Parallel():

    def __init__(self, env, lop, rop):
        self.env = env
        self.lop = lop
        self.rop = rop


    def excute(self):
        yield self.env.process(self.lop.excute()) & self.env.process(self.rop.excute())

        # Simulation.activate(self.lop, self.lop.excute(), Simulation.now())
        # Simulation.activate(self.rop, self.rop.excute(), Simulation.now())
        # yield Simulation.waitevent, self, [self.lop, self.rop]


        # if isinstance(self.lop, Stage):
        #     ls = self.lop.getReplica()
        #     lw = ls.getServiceTime()
        #     # yield Simulation.hold, self, lw
        #     if isinstance(self.rop, Stage):
        #         rs = self.rop.getReplica()
        #         rw = rs.getServiceTime()
        #         self.wait = max(lw, rw)
        #         yield Simulation.hold, self, self.wait
        #     else:
        #         self.rop.excute()
        #         self.wait = max(lw, self.rop.getWait())
        #         yield Simulation.hold, self, self.wait
        # else:
        #     self.lop.excute()
        #     if isinstance(self.rop, Stage):
        #         rs = self.rop.getReplica()
        #         rw = rs.getServiceTime()
        #         self.wait = max(self.lop.getWait(), rw)
        #         yield Simulation.hold, self, self.wait
        #     else:
        #         self.rop.excute()
        #         self.wait = max(self.lop.getWait(), self.rop.getWait())
        #         yield Simulation.hold, self, self.wait