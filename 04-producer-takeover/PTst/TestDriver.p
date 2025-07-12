machine MultiReplicaWithFailure {
    start state Init {
        entry {
            var config: logConfig;
            config = (numReplicas=3,
                numLogEntries=10,
                numLogNodes=5,
                failParticipants=2,
                totalProducers=2);
            SetupLogSystem(config);
        }
    }
}

// type that represents the configuration of the system under test
type logConfig = (
    numReplicas: int,
    numLogEntries: int,
    numLogNodes: int,
    failParticipants: int,
    totalProducers: int
);

fun SetupLogSystem(config: logConfig) {
    var logNodes: set[LogNode];
    var producer: Producer;
    var takeOverProducer: Producer;
    var consumer: Consumer;
    var coordinator: Coordinator;
    var i: int;
    var logId: int;

    InitializeLoggingSpecs(config.numReplicas, config.numLogEntries, config.totalProducers);

    i = 0;
    while (i < config.numLogNodes) {
        logNodes += (new LogNode());
        i = i + 1;
    }
    coordinator = new Coordinator((logNodes = logNodes, numReplicas = config.numReplicas));
    consumer = new Consumer((logId = logId, totalNumLogEntries = config.numLogEntries - 1, coordinator = coordinator, finalSeqNum = config.totalProducers * config.numLogEntries - 2));

    // Create the failure injector if we want to inject failures
    if(config.failParticipants > 0)
    {
        CreateFailureInjectorWithNotification((nodes = logNodes, nFailures = config.failParticipants, coordinator = coordinator));
    }

    assert config.totalProducers > 0;
    IntializeCreateProducers((totalProducers = config.totalProducers, coordinator = coordinator, numLogEntries = config.numLogEntries, logId = logId));
}


fun InitializeLoggingSpecs(numReplicas: int, numEntries: int, totalProducers: int) {
    announce eMonitor_CommitDurability, numReplicas;
    announce eMonitor_Progress, (numEntries = numEntries, totalProducers = totalProducers);
    announce eMonitor_ReadCommitted;
    announce eMonitor_SingleProducer;
}

