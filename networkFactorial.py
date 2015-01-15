import itertools
import subprocess
import os
import sys

def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

uniqId = sys.argv[1]

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
switchSelectionStrategy = ["oracle", "oracle_rev_client", "oracle_probability", "oracle_all", "passive"]
c4Weight = [1]
numClients = [30]
numServers = [10]
numWorkload = [1]
workloadModel = ["poisson"]
serverConcurrency = [4]
serviceTime = [4]
# utilization = [0.4, 0.45]
interarrivalTime = [0.005]
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
seed = [int(uniqId)]
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

logFolder = "post-nsdi-tv-sweep" + uniqId
# logFolder = "paperSkewSweep" + uniqId

if not os.path.exists(logFolder):
        os.makedirs(logFolder)

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
        interarrivalTime,
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

print len(PARAM_COMBINATIONS)

basePath = os.getcwd()

expPrefix = 0 #our new experiment prefix

inputs = open(basePath+ "/logs/../%s/inputs" % (logFolder), "w+")
#inputs.write("blalbalba")
for combination in PARAM_COMBINATIONS:
        expPrefix += 1
        iLeaf, iSpine, hostsPerLeaf, leafHostBW, \
        spineLeafBW, procTime, valueSizeModel, \
        packetSize, switchBufferSize, switchForwardingStrategy, \
        switchSelectionStrategy, c4Weight, \
        numClients, numServers, numWorkload, \
        workloadModel, serverConcurrency, \
        serviceTime, interarrivalTime, \
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
                --interarrivalTime %s\
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
                     interarrivalTime,
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
        proc = subprocess.Popen(cmd.split(),
                                shell=False)
        out, err = proc.communicate()
        
        # write combinations to file for later processing by R
        inputs.write(' '.join(map(lambda x: str(x), combination)) + "\r\n")
        
        if (proc.returncode != 0):
            print ' '.join(map(lambda x: str(x), combination)) + " ERROR:" + err
        os.chdir(basePath)

inputs.close()
os.chdir(basePath + "/plotting")
cmd = "Rscript factorialResults2.r %s"\
    % (logFolder)
proc = subprocess.Popen(cmd.split(),
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
out, err = proc.communicate()
print out