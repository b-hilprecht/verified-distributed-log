machine MultiReplicaNoFailure {
    start state Init {
        entry {
            var config: logConfig;
            config = (numReplicas=3,
                numLogEntries=10,
                numLogNodes=3);
            SetUlogSystem(config);
        }
    }
}

// type that represents the configuration of the system under test
type logConfig = (
    numReplicas: int,
    numLogEntries: int,
    numLogNodes: int
);

fun SetUlogSystem(config: logConfig) {
    var logNodes: set[LogNode];
    var producer: Producer;
    var consumer: Consumer;
    var i: int;

    InitializeLoggingSpecs(config.numReplicas, config.numLogEntries);

    i = 0;
    while (i < config.numLogNodes) {
        logNodes += (new LogNode(i));
        i = i + 1;
    }
    
    consumer = new Consumer((logNode = logNodes[i-1], totalNumLogEntries = config.numLogEntries - 1));
    producer = new Producer((logNodes = logNodes, numEntries = config.numLogEntries));
}


fun InitializeLoggingSpecs(numReplicas: int, numEntries: int) {
    announce eMonitor_Durability, numReplicas;
    announce eMonitor_Progress, numEntries;
}

