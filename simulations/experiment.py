import SimPy.Simulation as Simulation
import server
import client
import workload


if __name__ == '__main__':

    # random.seed(12345)

    Simulation.initialize()
    servers = []

    for i in range(1):
        serv = server.Server(i, resourceCapacity=4, serviceTime=5)
        servers.append(serv)

    client = client.Client(id_="Client1",
                           serverList=servers,
                           replicaSelectionStrategy="PENDING",
                           accessPattern="uniform",
                           replicationFactor=2)

    latencyMonitor = Simulation.Monitor(name="Latency")

    for i in range(15):
        w = workload.Workload(i, latencyMonitor)
        Simulation.activate(w, w.run([client],
                                     "unthrottled",
                                     None), at=0.0)
    Simulation.simulate(until=100)

    # print client.pendingRequestsMonitor.yseries()
    for serv in servers:
        print "------- Server:%s %s ------" % (serv.id, "WaitMon")
        for entry in serv.queueResource.waitMon:
            print entry

        # print "------- Server:%s %s ------" % (serv.id, "ActMon")
        # for entry in serv.queueResource.actMon:
        #     print entry

    # print "------- Latency ------"
    # print "Mean:", latencyMonitor.mean()
    # for entry in latencyMonitor:
    #     print entry
