import SimPy.Simulation as Simulation
import random


def sort(cassandraProcess, originalReplicaSet, coordinatorNode):

    replica_set = original_replica_set[0:]

    if(cassandra_process.REPLICA_SELECTION_STRATEGY == "RANDOM"):
        # Pick a random node for the request.
        # Represents SimpleSnitch + uniform request access.
        # Ignore scores and everything else.
        random.shuffle(replica_set)

    elif(cassandra_process.REPLICA_SELECTION_STRATEGY == "PENDING"):
        # Sort by number of pending requests
        replica_set.sort(key=cassandra_process.pending_map[coordinator_node].get)

    elif(cassandra_process.REPLICA_SELECTION_STRATEGY == "RESPONSE_TIME"):
        # Sort by number of pending requests
        replica_set.sort(key=cassandra_process.response_times[coordinator_node].get)

    elif(cassandra_process.REPLICA_SELECTION_STRATEGY == "BALANCED_RESPONSE_TIME"):
        # Sort by number of pending requests
        balanced_response_times = {}
        for each in replica_set:
            balanced_response_time = 0
            if (len(cassandra_process.waiting_times[coordinator_node][each]) > 0):
                response_time = cassandra_process.response_times[coordinator_node][each]
                qlen = cassandra_process.pending_map[coordinator_node][each]
                balanced_response_time = response_time * qlen
                print response_time, qlen, balanced_response_time
            balanced_response_times[each] = balanced_response_time

        replica_set.sort(key=balanced_response_times.get)
    else:
        print cassandra_process.REPLICA_SELECTION_STRATEGY
        assert False, "cassandra_process.REPLICA_SELECTION_STRATEGY isn't set or is invalid"

    return replica_set
