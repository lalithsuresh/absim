import itertools
import subprocess
import os
import sys

def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

iLeaf = [12]
iSpine = [8]
hostsPerLeaf = [8] 
leafHostBW = [125]
spineLeafBW = [250]
procTime = [0.002]
valueSizeModel = ["pareto"]
packetSize = [0.00057]
switchBufferSize = [20]
switchForwardingStrategy = ["local"]
switchSelectionStrategy = ["oracle", "passive"]
c4Weight = [1]
numClients = [30]
numServers = [10]
numWorkload = [1]
workloadModel = ["poisson"]
serverConcurrency = [4]
serviceTime = [4]
utilization = [0.7]
serviceTimeModel = ["random.expovariate"]
replicationFactor = [10]
selectionStrategy = ["expDelay"]
rateInterval = [20]
cubicC = [0.000004]
cubicSmax = [10]
cubicBeta = [0.2]
hysterisisFactor = [2]
shadowReadRatio = [0.0]
accessPattern = ["uniform"]
nwLatencyBase = [0.005]
nwLatencyMu = [0.0]
nwLatencySigma = [0]
simulationDuration = [600000]
seed = [255]
numRequests = [5000]
# expScenario = ["heterogenousStaticServiceTimeScenario"]
expScenario = ["timeVaryingServiceTimeServers"]
# expScenario = ["base"]
demandSkew = [0.2]
highDemandFraction = [0.8]
# slowServerFraction = [0.3, 0.5, 0.7]
# slowServerSlowness = [0.8, 0.5, 0.3]
# intervalParam = [0]
# timeVaryingDrift = [0]
slowServerFraction = [0.8]
slowServerSlowness = [0.9]
intervalParam = [500]
timeVaryingDrift = [5]
switchRateLimiter = [True]

LIST = [iLeaf,
        iSpine,
        hostsPerLeaf,
        leafHostBW,
        spineLeafBW,
        procTime,
        valueSizeModel,
        packetSize,
        switchBufferSize,
        switchForwardingStrategy,
        switchSelectionStrategy,
        c4Weight,
        numClients,
        numServers,
        numWorkload,
        workloadModel,
        serverConcurrency,
        serviceTime,
        utilization,
        serviceTimeModel,
        replicationFactor,
        selectionStrategy,
        rateInterval,
        cubicC,
        cubicSmax,
        cubicBeta,
        hysterisisFactor,
        shadowReadRatio,
        accessPattern,
        nwLatencyBase,
        nwLatencyMu,
        nwLatencySigma,
        simulationDuration,
        seed,
        numRequests,
        expScenario,
        demandSkew,
        highDemandFraction,
        slowServerFraction,
        slowServerSlowness,
        intervalParam,
        timeVaryingDrift,
        switchRateLimiter,
        ]

PARAM_COMBINATIONS = list(itertools.product(*LIST))

basePath = os.getcwd()
        
expPrefix = 0 #our new experiment prefix

#inputs.write("blalbalba")
for combination in PARAM_COMBINATIONS:
        expPrefix += 1
        iLeaf, iSpine, hostsPerLeaf, leafHostBW, \
        spineLeafBW, procTime, valueSizeModel, \
        packetSize, switchBufferSize, switchForwardingStrategy, \
        switchSelectionStrategy, c4Weight, \
        numClients, numServers, numWorkload, \
        workloadModel, serverConcurrency, \
        serviceTime, utilization, \
        serviceTimeModel, replicationFactor, selectionStrategy, \
        rateInterval, cubicC, cubicSmax, \
        cubicBeta, hysterisisFactor, shadowReadRatio, \
        accessPattern, nwLatencyBase, \
        nwLatencyMu, nwLatencySigma, \
        simulationDuration, seed, \
        numRequests, expScenario, \
        demandSkew, highDemandFraction, \
        slowServerFraction, \
        slowServerSlowness, intervalParam, \
        timeVaryingDrift, switchRateLimiter= combination

        os.chdir(basePath + "/simulations")
        backpressure = ""

        logFolder = '_'.join(map(lambda x : str(x), combination))
        logFolder = logFolder.replace('[', '').replace(']', '').replace(', ', '_')
        logFolder = "logs/" + logFolder
        
        if (selectionStrategy == "expDelay"):
            backpressure = "--backpressure"
        
        srl = ""
        
        if (switchRateLimiter):
            srl = "--switchRateLimiter"

        cmd = "python networkExperiment.py \
                --iLeaf %s\
                --iSpine %s\
                --hostsPerLeaf %s\
                --leafHostBW %s\
                --spineLeafBW %s\
                --procTime %s\
                --valueSizeModel %s\
                --packetSize %s\
                --switchBufferSize %s\
                --switchForwardingStrategy %s\
                --switchSelectionStrategy %s\
                --c4Weight %s\
                --numClients %s\
                --numServers %s\
                --numWorkload %s\
                --workloadModel %s\
                --serverConcurrency %s\
                --serviceTime %s\
                --utilization %s\
                --serviceTimeModel %s\
                --replicationFactor %s\
                --selectionStrategy %s \
                --shadowReadRatio %s\
                --rateInterval %s\
                --cubicC %s\
                --cubicSmax %s\
                --cubicBeta %s\
                --hysterisisFactor %s\
                --accessPattern %s\
                --nwLatencyBase %s\
                --nwLatencyMu %s\
                --nwLatencySigma %s\
                --expPrefix %s\
                --simulationDuration %s\
                --seed %s\
                --numRequests %s\
                --expScenario %s\
                --demandSkew %s\
                --highDemandFraction %s\
                --slowServerFraction %s\
                --slowServerSlowness %s\
                --intervalParam %s\
                --timeVaryingDrift %s\
                --logFolder %s %s %s"\
                  % (iLeaf,
                    iSpine,
                    hostsPerLeaf,
                    leafHostBW,
                    spineLeafBW,
                    procTime,
                    valueSizeModel,
                    packetSize,
                    switchBufferSize,
                    switchForwardingStrategy,
                    switchSelectionStrategy,
                    c4Weight,            
                     numClients,
                     numServers,
                     numWorkload,
                     workloadModel,
                     serverConcurrency,
                     serviceTime,
                     utilization,
                     serviceTimeModel,
                     replicationFactor,
                     selectionStrategy,
                     shadowReadRatio,
                     rateInterval,
                     cubicC,
                     cubicSmax,
                     cubicBeta,
                     hysterisisFactor,
                     accessPattern,
                     nwLatencyBase,
                     nwLatencyMu,
                     nwLatencySigma,
                     expPrefix,
                     simulationDuration,
                     seed,
                     numRequests,
                     expScenario,
                     demandSkew,
                     highDemandFraction,
                     slowServerFraction,
                     slowServerSlowness,
                     intervalParam,
                     timeVaryingDrift,
                     logFolder,
                     backpressure,
                     srl)
                  
        toPrint = "cd c4 && mkdir -p " + logFolder + " && cd simulations && " + cmd + " > ../%s/%s_summary.txt 2>&1;"%(logFolder,selectionStrategy)
        print toPrint
