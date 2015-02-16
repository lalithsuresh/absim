"Create by Jing Li"


import simpy


class Sequential():

    def __init__(self, env, lop, rop=None):
        self.env = env
        self.lop = lop
        self.rop = rop

    def capacity(self):
        capacity = self.lop.capacity()
        if self.rop is not None:
            right = self.rop.capacity()
            if right < capacity:
                capacity = right
        return capacity

    def execute(self):
        try:
            yield self.env.process(self.lop.execute())
            if self.rop is not None:
                yield self.env.process(self.rop.execute())
        except simpy.Interrupt as i:
            pass


        # self.lop.execute()
        # wait(event_lop)
        # self.rop.execute()
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
        #         self.rop.execute()
        #         self.wait = lw + self.rop.getWait()
        # else:
        #     self.lop.execute()
        #     if isinstance(self.rop, Stage):
        #         rs = self.rop.getReplica()
        #         rw = rs.getServiceTime()
        #         yield Simulation.hold, self, rw
        #         self.wait = self.lop.getWait() + rw
        #     else:
        #         self.rop.execute()
        #         self.wait = self.lop.getWait() + self.rop.getWait()