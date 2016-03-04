'''
Created on Oct 11, 2014

@author: Waleed Reda
'''

from link import Link
from switch import Switch
from server import Server
from client import Client
import random
#import networkx as nx
#import matplotlib.pyplot as plt
#import pylab
#import math

class LeafSpinTopology():
    SpineSwitchList = []
    LeafSwitchList = []
    HostList = []
    ClientList = []
    ServerList = []
    def __init__(self, iSpine, iLeaf, hostsPerLeaf, spineLeafBW,
                 leafHostBW, procTime, ClientList,
                 ServerList, selectionStrategy, forwardingStrategy, c4Weight,
                 rateInterval, cubicC, cubicSmax, cubicBeta, hysterisisFactor,
                 rateLimiterEnabled):
        print 'starting topo..'
        self.iSpine = iSpine
        self.iLeaf = iLeaf
        self.hostsPerLeaf = hostsPerLeaf
        self.spineLeafBW = spineLeafBW
        self.leafHostBW = leafHostBW
        self.procTime = procTime
        self.selectionStrategy = selectionStrategy
        self.forwardingStrategy = forwardingStrategy
        self.c4Weight = c4Weight
        self.links = {}
        self.ClientList = ClientList
        self.ServerList = ServerList
        self.rateInterval = rateInterval
        self.cubicC = cubicC
        self.cubicSmax = cubicSmax
        self.cubicBeta = cubicBeta
        self.hysterisisFactor = hysterisisFactor
        self.rateLimiterEnabled = rateLimiterEnabled

    def createTopo(self):    
        self.createSpineLayerSwitch(self.iSpine)
        self.createLeafLayerSwitch(self.iLeaf)
        for c in self.ClientList:
            self.HostList.append(c)
        for s in self.ServerList:
            self.HostList.append(s)
        self.createLinks()
        self.updateConnections()
        self.updatePaths()
        
    """
    Create Switch and Host
    """

    def createSpineLayerSwitch(self, NUMBER):
        for x in range(1, NUMBER+1):
            c = Switch(id_="Spine%s" % (x), htype="spine", procTime=self.procTime,
                       clientList= self.ClientList, serverList=self.ServerList,
                       selectionStrategy=self.selectionStrategy,
                       forwardingStrategy=self.forwardingStrategy, c4Weight = self.c4Weight,
                       rateInterval = self.rateInterval, cubicC = self.cubicC,
                       cubicSmax = self.cubicSmax, cubicBeta = self.cubicBeta,
                       hysterisisFactor = self.hysterisisFactor, rateLimiterEnabled = self.rateLimiterEnabled)
            self.SpineSwitchList.append(c)

    def createLeafLayerSwitch(self, NUMBER):
        for x in range(1, NUMBER+1):
            c = Switch(id_="Leaf%s" % (x), htype="leaf", procTime=self.procTime,
                       clientList= self.ClientList, serverList=self.ServerList,
                       selectionStrategy=self.selectionStrategy,
                       forwardingStrategy=self.forwardingStrategy, c4Weight = self.c4Weight,
                       rateInterval = self.rateInterval, cubicC = self.cubicC,
                       cubicSmax = self.cubicSmax, cubicBeta = self.cubicBeta,
                       hysterisisFactor = self.hysterisisFactor, rateLimiterEnabled = self.rateLimiterEnabled)
            self.LeafSwitchList.append(c)
    

    """
    Create Link 
    """
    def createLinks(self):
        for x in range(0, self.iSpine):
            for y in range(0, self.iLeaf):
                self.addLink(self.SpineSwitchList[x], self.LeafSwitchList[y], bw=self.spineLeafBW)

        for x in range(0, len(self.HostList)):
            r = random.randint(0, len(self.LeafSwitchList)-1)
            self.addLink(self.LeafSwitchList[r], self.HostList[x], bw=self.leafHostBW)

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
                
        for lsw in self.LeafSwitchList:
            usws = lsw.getUppers()
            for usw in usws:
                usw.addConnectedHosts(lsw, lsw.getConnectedHosts())

    """
    Update Paths for leaf switches
    """
    def updatePaths(self):
        for lsw in self.LeafSwitchList:
            lsw.trackPaths()

        #for c in self.CoreSwitchList:
        #    print c.getConnectedHosts()
            
    def getBW(self, n1, n2):
        if(type(n1).__name__ == 'Host' or type(n2).__name__ == 'Host'):
            return self.leafHostBW
        else:
            return self.spineLeafBW
        
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
    
    def getSwitches(self):
        return self.LeafSwitchList + self.SpineSwitchList
    
    #Draws the specified Fat-Tree using the NetworkX library
    #===========================================================================
    # def draw(self):
    #     val_map = {
    #         'core': 1.0,
    #         'edge': 1.0,
    #         'aggr': 1.0,
    #         'client': 0.8,
    #         'server': 0.9}
    #     G = nx.Graph()
    #     for n in self.links.keys():
    #         G.add_node(n.id, type=n.htype)      
    #     for n in self.links.keys():
    #         for l in self.links[n]:
    #             G.add_edge(n.id, l.dst.id, weight=l.bw)
    #     pos=nx.spring_layout(G)
    #     edge_labels=dict([((u,v,),'bw='+str(d['weight']))
    #          for u,v,d in G.edges(data=True)])
    #     values = [val_map.get(node[1]['type'], 0.1) for node in G.nodes(True)]
    #     # use one of the edge properties to control line thickness
    #     edgewidth = [ math.log(d['weight'], 2) for (u,v,d) in G.edges(data=True)]
    #     nx.draw_networkx_edges(G, pos, width=edgewidth,)
    #     nx.draw(G, pos, cmap = plt.get_cmap('jet'), node_color = values, with_labels=True, node_size = 500, font_size =5)
    #     #nx.draw_networkx_edge_labels(G,pos,edge_labels=edge_labels)
    #     pylab.show()
    #===========================================================================
