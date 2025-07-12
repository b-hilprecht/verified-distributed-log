machine MultiReplicaWithFailure {
    start state Init {
        entry {
            var config: logConfig;
            config = (numReplicas=3,
                numLogEntries=10,
                numLogNodes=5,
                failParticipants=2);
            SetupLogSystem(config);
        }
    }
}

// type that represents the configuration of the system under test
type logConfig = (
    numReplicas: int,
    numLogEntries: int,
    numLogNodes: int,
    failParticipants: int
);

fun SetupLogSystem(config: logConfig) {
    var logNodes: set[LogNode];
    var producer: Producer;
    var consumer: Consumer;
    var coordinator: Coordinator;
    var i: int;
    var logId: int;

    InitializeLoggingSpecs(config.numReplicas, config.numLogEntries);

    i = 0;
    while (i < config.numLogNodes) {
        logNodes += (new LogNode());
        i = i + 1;
    }
    coordinator = new Coordinator((logNodes = logNodes, numReplicas = config.numReplicas));
    producer = new Producer((logId = logId, coordinator = coordinator, numEntries = config.numLogEntries));
    consumer = new Consumer((logId = logId, totalNumLogEntries = config.numLogEntries - 1, coordinator = coordinator));
    
    // Create the failure injector if we want to inject failures
    if(config.failParticipants > 0)
    {
        CreateFailureInjectorWithNotification((nodes = logNodes, nFailures = config.failParticipants, coordinator = coordinator));
    }
}


fun InitializeLoggingSpecs(numReplicas: int, numEntries: int) {
    announce eMonitor_CommitDurability, numReplicas;
    announce eMonitor_Progress, numEntries;
    announce eMonitor_ReadCommitted;
}

