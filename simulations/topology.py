'''
Created on Oct 11, 2014

@author: Waleed Reda
'''

from link import Link
from switch import Switch
from server import Server
from client import Client
import networkx as nx
import matplotlib.pyplot as plt
import pylab
import math

class Topology():
    CoreSwitchList = []
    AggrSwitchList = []
    EdgeSwitchList = []
    HostList = []
    ClientList = []
    ServerList = []
    def __init__(self, args):
        print 'starting topo..'
        self.args = args
        self.iNUMBER = self.args.iNumber
        self.iCoreLayerSwitch = self.iNUMBER
        self.iAggrLayerSwitch = self.iNUMBER * 2
        self.iEdgeLayerSwitch = self.iNUMBER * 2
        self.iHost = self.iEdgeLayerSwitch * 2
        self.links = {}

    def createTopo(self):    
        self.createCoreLayerSwitch(self.iCoreLayerSwitch)
        self.createAggrLayerSwitch(self.iAggrLayerSwitch)
        self.createEdgeLayerSwitch(self.iEdgeLayerSwitch)
        if(self.args.placementStrategy == 'interleave'):                        
            self.createServer(self.iHost/2)
            self.createClient(self.iHost/2)
            for i in xrange(0, self.iHost/2):
                self.HostList.append(self.ClientList[i])
                self.HostList.append(self.ServerList[i])
        self.createLink()
        self.updateConnections()
        
    """
    Create Switch and Host
    """

    def createCoreLayerSwitch(self, NUMBER):
        for x in range(1, NUMBER+1):
            c = Switch(id_="Core%s" % (x), htype="core", procTime=self.args.procTime)
            self.CoreSwitchList.append(c)

    def createAggrLayerSwitch(self, NUMBER):
        for x in range(1, NUMBER+1):
            c = Switch(id_="Aggr%s" % (x), htype="aggr", procTime=self.args.procTime)
            self.AggrSwitchList.append(c)

    def createEdgeLayerSwitch(self, NUMBER):
        for x in range(1, NUMBER+1):
            c = Switch(id_="Edge%s" % (x), htype="edge", procTime=self.args.procTime)
            self.EdgeSwitchList.append(c)
    
    def createClient(self, NUMBER):
        for x in range(1, NUMBER+1):
            #print 'server list', self.ServerList
            c = Client(id_="Client%s" % (x),
                          serverList=self.ServerList,
                          replicaSelectionStrategy=self.args.selectionStrategy,
                          accessPattern=self.args.accessPattern,
                          replicationFactor=self.args.replicationFactor,
                          backpressure=self.args.backpressure,
                          shadowReadRatio=self.args.shadowReadRatio)
            self.ClientList.append(c) 

    def createServer(self, NUMBER):
        # Start the servers
        for x in range(1, NUMBER+1):
            serv = Server(id_="Server%s" % (x),
                             resourceCapacity=self.args.serverConcurrency,
                             serviceTime=(self.args.serviceTime),
                             serviceTimeModel=self.args.serviceTimeModel,
                             valueSizeModel = self.args.valueSizeModel)
            #Simulation.activate(mup, mup.run(), at=0.0) #no need to have service times as a rv
            self.ServerList.append(serv)
    """
    Create Link 
    """
    def createLink(self):
        for x in range(0, self.iAggrLayerSwitch, 2):
            self.addLink(self.CoreSwitchList[0], self.AggrSwitchList[x], bw=self.args.coreAggrBW)
            self.addLink(self.CoreSwitchList[1], self.AggrSwitchList[x], bw=self.args.coreAggrBW)
        for x in range(1, self.iAggrLayerSwitch, 2):
            self.addLink(self.CoreSwitchList[2], self.AggrSwitchList[x], bw=self.args.coreAggrBW)
            self.addLink(self.CoreSwitchList[3], self.AggrSwitchList[x], bw=self.args.coreAggrBW)
        
        for x in range(0, self.iAggrLayerSwitch, 2):
            self.addLink(self.AggrSwitchList[x], self.EdgeSwitchList[x], bw=self.args.aggrEdgeBW)
            self.addLink(self.AggrSwitchList[x], self.EdgeSwitchList[x+1], bw=self.args.aggrEdgeBW)
            self.addLink(self.AggrSwitchList[x+1], self.EdgeSwitchList[x], bw=self.args.aggrEdgeBW)
            self.addLink(self.AggrSwitchList[x+1], self.EdgeSwitchList[x+1], bw=self.args.aggrEdgeBW)

        for x in range(0, self.iEdgeLayerSwitch):
            ## limit = 2 * x + 1 
            self.addLink(self.EdgeSwitchList[x], self.HostList[2 * x], bw=self.args.edgeHostBW)
            self.addLink(self.EdgeSwitchList[x], self.HostList[2 * x + 1], bw=self.args.edgeHostBW)

    def addLink(self, n1, n2, bw):
        #print "Adding link between %s:%s and %s:%s"%(n1.id, n1.htype, n2.id, n2.htype)
        if(n1 not in self.links):
            self.links[n1] = [Link(bw, n2)]
        else:
            self.links[n1].append(Link(bw, n2))
        if(n2 not in self.links):
            self.links[n2] = [Link(bw, n1)]
        else:
            self.links[n2].append(Link(bw, n1))
        n1.addNeighbor(n2, bw)
        n2.addNeighbor(n1, bw)

        #=======================================================================
        # if(n1.isAggr() and n2.isEdge()):
        #     n1.addConnectedHosts(n2, n2.getHosts())
        # elif(n1.isEdge() and n2.isAggr()):
        #     n2.addConnectedHosts(n1, n1.getHosts())
        #=======================================================================

    """
    Update connections (from lower to higher switches)
    """
    def updateConnections(self):
        for h in self.HostList:
            usws = h.getUppers()
            for usw in usws:
                usw.addConnectedHosts(False, [h]) #no intermediary
                
        for esw in self.EdgeSwitchList:
            usws = esw.getUppers()
            for usw in usws:
                usw.addConnectedHosts(esw, esw.getConnectedHosts())

        for asw in self.AggrSwitchList:
            usws = asw.getUppers()
            for usw in usws:
                usw.addConnectedHosts(asw, asw.getConnectedHosts())
        #for c in self.CoreSwitchList:
        #    print c.getConnectedHosts()
            
    def getBW(self, n1, n2):
        if(type(n1).__name__ == 'Host' or type(n2).__name__ == 'Host'):
            return self.edgeHostBW
        elif((n1.isCore() or n2.isCore())):
            return self.coreAggrBW
        else:
            return self.aggrEdgeBW
        
    #Dynamic Load-Balaning algorithm (DLB) from OpenFlow based Load Balancing for Fat-Tree Networks with Multipath Support
    #[MOVED TO SWITCH CLASS]
    def getNextHop(self, src, dst):
        #check if I'm direct neighbors with dst
        if(src.isNeighbor(dst)):
            egressPort = src.getPort(dst)
        #check if I'm connected to dst through intermediary node
        elif(src.getIntermediary(dst)):
            nextHop = src.getIntermediary(dst)
            egressPort = src.getPort(nextHop)
        else: 
            #We're going up!
            possible_hops = src.getUppers()
            #[TODO] Apply heuristic
            nextHop = possible_hops[0]
            egressPort = src.getPort(nextHop)
        return egressPort
    
    #Draws the specified Fat-Tree using the NetworkX library
    def draw(self):
        val_map = {
            'core': 1.0,
            'edge': 1.0,
            'aggr': 1.0,
            'client': 0.8,
            'server': 0.9}
        G = nx.Graph()
        for n in self.links.keys():
            G.add_node(n.id, type=n.htype)      
        for n in self.links.keys():
            for l in self.links[n]:
                G.add_edge(n.id, l.dst.id, weight=l.bw)
        pos=nx.spring_layout(G)
        edge_labels=dict([((u,v,),'bw='+str(d['weight']))
             for u,v,d in G.edges(data=True)])
        values = [val_map.get(node[1]['type'], 0.1) for node in G.nodes(True)]
        # use one of the edge properties to control line thickness
        edgewidth = [ math.log(d['weight'], 2) for (u,v,d) in G.edges(data=True)]
        nx.draw_networkx_edges(G, pos, width=edgewidth,)
        nx.draw(G, pos, cmap = plt.get_cmap('jet'), node_color = values, with_labels=True, node_size = 500, font_size =5)
        #nx.draw_networkx_edge_labels(G,pos,edge_labels=edge_labels)
        pylab.show()
