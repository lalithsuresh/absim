import SimPy.Simulation as Simulation
import server
import client
import workload


def printMonitorTimeSeriesToFile(filename, prefix, monitor):
    outputFile = open(filename, 'w')
    for entry in monitor:
        outputFile.write("%s %s %s\n" % (prefix, entry[0], entry[1]))

    outputFile.close()


if __name__ == '__main__':

    # random.seed(12345)

    Simulation.initialize()
    servers = []

    for i in range(2):
        serv = server.Server(i, resourceCapacity=4, serviceTime=2)
        servers.append(serv)

    client = client.Client(id_="Client1",
                           serverList=servers,
                           replicaSelectionStrategy="PENDING",
                           accessPattern="uniform",
                           replicationFactor=2)

    latencyMonitor = Simulation.Monitor(name="Latency")

    for i in range(40):
        w = workload.Workload(i, latencyMonitor)
        Simulation.activate(w, w.run([client],
                                     "poisson",
                                     None), at=0.0)
    Simulation.simulate(until=100)

    #
    # Print a bunch of timeseries
    #

    printMonitorTimeSeriesToFile("logs/PendingRequests", client.id,
                                 client.pendingRequestsMonitor)
    for serv in servers:
        printMonitorTimeSeriesToFile("logs/WaitMon", serv.id,
                                     serv.queueResource.waitMon)
        printMonitorTimeSeriesToFile("logs/ActMon", serv.id,
                                     serv.queueResource.actMon)
        print "------- Server:%s %s ------" % (serv.id, "WaitMon")
        print "Mean:", serv.queueResource.waitMon.mean()

        print "------- Server:%s %s ------" % (serv.id, "ActMon")
        print "Mean:", serv.queueResource.actMon.mean()

    print "------- Latency ------"
    print "Mean:", latencyMonitor.mean()
    printMonitorTimeSeriesToFile("logs/Latency", "0",
                                 latencyMonitor)
