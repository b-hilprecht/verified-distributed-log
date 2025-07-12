// New segment
// previousSegmentId == -1 if this is the first log, logIds start with 0
type tNewSegment = (client: Producer, logId: tLogId, previousSegment: tSegmentId, previousSegmentNumEntries: int, lastEntry: tLogEntry);
enum tNewSegmentResponseStatus { NEW_SEG_OK, NEW_SEG_ALREADY_APPENDED, LOG_IN_TAKEOVER }
type tNewSegmentResponse = (status: tNewSegmentResponseStatus, logId: tLogId, newSegment: tSegmentId, nodes: set[LogNode], producer: Producer);
event eNewSegment : tNewSegment;
event eNewSegmentResponse : tNewSegmentResponse;

// New producer continues to write to the log.
type tTakeOverLog = (client: Producer, logId: tLogId);
event eTakeOverLog : tTakeOverLog;

// End segment. Also contains how many log entries can be considered committed in the segment.
type tEndSegment = (coordinator: Coordinator, logId: tLogId, segment: tSegmentId, numEntries: int, lastEntry: tLogEntry, newProducer: Producer);
event eEndSegment : tEndSegment;
type tEndSegmentResponse = (node: LogNode, logId: tLogId, segment: tSegmentId, numEntries: int, lastEntry: tLogEntry, newProducer: Producer);
event eEndSegmentResponse : tEndSegmentResponse;

// eSuspendSegment
type tSuspendSegment = (coordinator: Coordinator, logId: tLogId, segment: tSegmentId, producer: Producer, nodes: set[LogNode]);
event eSuspendSegment : tSuspendSegment;
type tExpectedSuspend = (segment: tSegmentId, producer: Producer, nodes: set[LogNode]);
type tSuspendSegmentResponse = (logId: tLogId, segment: tSegmentId, producer: Producer, nodes: set[LogNode], numEntries: int, lastEntry: tLogEntry);
event eSuspendSegmentResponse : tSuspendSegmentResponse;

// Segment state
type tSegmentState = (client: Consumer, logId: tLogId, segment: tSegmentId);
event eSegmentState : tSegmentState;
type tSegmentStateResponse = (status: tSegmentStatus, nodes: set[LogNode], logId: tLogId, segment: tSegmentId);
event eSegmentStateResponse : tSegmentStateResponse;

// Useful types
enum tLastSegmentStatus { IS_LAST, NOT_LAST }
type Segment = (logNodes: set[LogNode], producer: Producer);
type tLastLogSegment = (status: tLastSegmentStatus, nodes: set[LogNode], producer: Producer);

// technical events to know once the simulation is done
event eProducersDone;
event eConsumersDone;

/*
Coordination service managing which log segments get written to which nodes
*/
machine Coordinator {
    // All log nodes available to store log entries
    var logNodes: set[LogNode];
    // List of segments per logId
    var logs: map[tLogId, seq[Segment]];
    var currentLogTakeovers: map[tLogId, tExpectedSuspend];

    // We need to make sure the information that a segment is closed is eventually
    // passed on to the log nodes. This is for retrying such requests.
    var unacknowledgedEndSegments: set[tEndSegmentResponse];
    // Number of replicas for a log entry to be considered committed
    var numReplicas: int;
    var timer: Timer;

    // Technicalities of the simulation. We keep track if producers and consumers are
    // done to shut down the remaining machines and avoid false-positive liveness issues.
    var consumersDone: bool;
    var producersDone: bool;
    // Keep track of which nodes have failed. In a real system we would obtain this info
    // from a control plane / failure detector. This is not strictly needed for the coordinator
    // to work but reduces simulation time since we do not try to open segments on failed log
    // nodes.
    var failedNodes: set[LogNode];

    start state Init {
        entry (payload: (logNodes: set[LogNode], numReplicas: int)) {
            logNodes = payload.logNodes;
            numReplicas = payload.numReplicas;
            timer = CreateTimer(this);
            goto Manage;
        }
    }

    state Manage {
        on eNewSegment do (req: tNewSegment) {
            var i: int;
            var lastLogSegment: tLastLogSegment;

            // Ignore eNewSegment when the log is currently being taken over by a new node
            if (req.logId in currentLogTakeovers) {
                UnreliableSend(req.client, eNewSegmentResponse, (status = LOG_IN_TAKEOVER, logId = req.logId, newSegment = req.previousSegment + 1, nodes = default(set[LogNode]), producer = currentLogTakeovers[req.logId].producer));
                return;
            }

            lastLogSegment = IsLastSegment(req.logId, req.previousSegment);

            if (lastLogSegment.status == NOT_LAST) {
                // Could be that this is a retry to create the new segment
                if (lastLogSegment.producer == req.client) {
                    UnreliableSend(req.client, eNewSegmentResponse, (status = NEW_SEG_OK, logId = req.logId, newSegment = req.previousSegment + 1, nodes = lastLogSegment.nodes, producer = lastLogSegment.producer));
                    return;      
                }
                UnreliableSend(req.client, eNewSegmentResponse, (status = NEW_SEG_ALREADY_APPENDED, logId = req.logId, newSegment = req.previousSegment + 1, nodes = default(set[LogNode]), producer = lastLogSegment.producer));
                return;
            }
            // For the first log segment, we do not have to end any previous p log segments
            // and can directly acknowledge the creation.
            if (req.previousSegment == -1) {
                AcknowledgeNewSegment(req.client, req.logId);
                return;
            }
            
            // The coordinator makes sure to only acknowledge a new segment once at least one 
            // log node confirms that the previous segment is closed. At this point, all writes
            // to the previous segment will fail (since the consumer expects all log nodes to 
            // reply with ok for a write to succeed).
            // Along with this request to end the previous log segment the producer also informs
            // how many entries in the segment can be considered committed. This then decides if
            // the last log entry will be returned to consumers or truncated.
            BroadcastToNodes(lastLogSegment.nodes, eEndSegment, (coordinator = this, logId = req.logId, segment = req.previousSegment, numEntries = req.previousSegmentNumEntries, lastEntry = req.lastEntry, newProducer = req.client));
            
            // Keep track of which end segment requests need to be retried
            if (sizeof(unacknowledgedEndSegments) == 0)
                StartTimer(timer);
            while (i < sizeof(lastLogSegment.nodes)) {
                unacknowledgedEndSegments += ((node = lastLogSegment.nodes[i], logId = req.logId, segment = req.previousSegment, numEntries = req.previousSegmentNumEntries, lastEntry = req.lastEntry, newProducer = req.client));
                i = i + 1;
            }
        }

        on eTakeOverLog do (req: tTakeOverLog) {
            var numLogSegments: int;
            var lastLogSegment: Segment;

            if (!(req.logId in logs)) {
                AcknowledgeNewSegment(req.client, req.logId);
                return;
            }

            numLogSegments = sizeof(logs[req.logId]);
            if (numLogSegments == 0) {
                AcknowledgeNewSegment(req.client, req.logId);
                return;
            }

            // Probably a retry
            if (req.logId in currentLogTakeovers && currentLogTakeovers[req.logId].segment == numLogSegments - 1) {
                return;
            }

            lastLogSegment = logs[req.logId][numLogSegments - 1];
            if (lastLogSegment.producer == req.client) {
                UnreliableSend(req.client, eNewSegmentResponse, (status = NEW_SEG_OK, logId = req.logId, newSegment = numLogSegments - 1, nodes = lastLogSegment.logNodes, producer = lastLogSegment.producer));
                return;
            }

            if (sizeof(currentLogTakeovers) == 0)
                StartTimer(timer);
            currentLogTakeovers[req.logId] = (segment =  numLogSegments - 1, producer = req.client, nodes = lastLogSegment.logNodes);
            BroadcastToNodes(lastLogSegment.logNodes, eSuspendSegment, (coordinator = this, logId = req.logId, segment = numLogSegments - 1, nodes = lastLogSegment.logNodes, producer = req.client));
        }

        on eSuspendSegmentResponse do (req: tSuspendSegmentResponse) {
            var lastLogSegment: tLastLogSegment;
            var i: int;
            
            lastLogSegment = IsLastSegment(req.logId, req.segment);
            if (lastLogSegment.status != IS_LAST)
                return;
                
            
            if (!(req.logId in currentLogTakeovers)) 
                return;

            // We cannot know whether the last entry was successfully replicated to all log nodes. We therefore
            // replicate the last entry on behalf of the producer. To make sure our specs do not hit false positives
            // we announce this log entry as durably replicated. This is fine since the coordinator relies on a 
            // consensus service.
            if (req.numEntries > 0) {
                announce eDurableInCoordinator, (seqNum = req.lastEntry.seqNum, logId = req.logId);
                announce eProducerTakeOver, (logId = req.logId, producer = req.producer);
                announce eLogEntryCommitted, (producer = req.producer, seqNum = req.lastEntry.seqNum, logId = req.logId);
            }

            BroadcastToNodes(lastLogSegment.nodes, eEndSegment, (coordinator = this, logId = req.logId, segment = req.segment, numEntries = req.numEntries, lastEntry = req.lastEntry, newProducer = req.producer));
        
            // Keep track of which end segment requests need to be retried
            if (sizeof(unacknowledgedEndSegments) == 0)
                StartTimer(timer);
            while (i < sizeof(lastLogSegment.nodes)) {
                unacknowledgedEndSegments += ((node = lastLogSegment.nodes[i], logId = req.logId, segment = req.segment, numEntries = req.numEntries, lastEntry = req.lastEntry, newProducer = req.producer));
                i = i + 1;
            }
            
            AcknowledgeNewSegment(req.producer, req.logId);
            currentLogTakeovers -= (req.logId);         
        }

        on eEndSegmentResponse do (req: tEndSegmentResponse) {
            assert req.logId in logs;

            unacknowledgedEndSegments -= (req);
            
            // it should concern the last log segment
            if (req.segment != sizeof(logs[req.logId])-1) {
                return;
            }

            AcknowledgeNewSegment(req.newProducer, req.logId);
        }

        on eSegmentState do (req: tSegmentState) {
            var segmentNodes: set[LogNode];
            if (!(req.logId in logs)) {
                UnreliableSend(req.client, eSegmentStateResponse, (status = SEG_NOT_INIT, nodes = default(set[LogNode]), logId = req.logId, segment = req.segment));
                return;
            }
            
            if (req.segment >= sizeof(logs[req.logId])) {
                print format("Coordinator eSegmentState: segment {0} SEG_NOT_INIT", req.segment);
                UnreliableSend(req.client, eSegmentStateResponse, (status = SEG_NOT_INIT, nodes = default(set[LogNode]), logId = req.logId, segment = req.segment));
                return;
            }
            
            print format("Coordinator eSegmentState: segment {0} SEG_OPEN", req.segment);
            segmentNodes = logs[req.logId][req.segment].logNodes;
            UnreliableSend(req.client, eSegmentStateResponse, (status = SEG_OPEN, nodes = segmentNodes, logId = req.logId, segment = req.segment));
        }

        on eTimeOut do {
            var newUnacknowledgedEndSegments: set[tEndSegmentResponse];
            var segmentAck: tEndSegmentResponse;
            var i: int;
            var logId: int;
            var expectedSuspend: tExpectedSuspend;

            // Retry unacknowledged end segments. Skip those directed to failed nodes
            while (i < sizeof(unacknowledgedEndSegments)) {
                segmentAck = unacknowledgedEndSegments[i];

                if (segmentAck.node in failedNodes) {
                    i = i + 1;
                    continue;
                }

                UnreliableSend(segmentAck.node, eEndSegment, (coordinator = this, logId = segmentAck.logId, segment = segmentAck.segment, numEntries = segmentAck.numEntries, lastEntry = segmentAck.lastEntry, newProducer = segmentAck.newProducer));
                newUnacknowledgedEndSegments += (segmentAck);
                i = i + 1;
            }

            // Retry unacknowledged takeovers
            i = 0;
            while (i < sizeof(keys(currentLogTakeovers))) {
                logId = keys(currentLogTakeovers)[i];
                expectedSuspend = currentLogTakeovers[logId];
                
                BroadcastToNodes(expectedSuspend.nodes, eSuspendSegment, (coordinator = this, logId = logId, segment = expectedSuspend.segment, producer = expectedSuspend.producer, nodes = expectedSuspend.nodes));
                i = i + 1;
            }

            if (sizeof(newUnacknowledgedEndSegments) > 0 || sizeof(currentLogTakeovers) > 0)
                StartTimer(timer);
            unacknowledgedEndSegments = newUnacknowledgedEndSegments;
        }

        on eInformNodeFailure do (failingNode: LogNode) {
            logNodes -= (failingNode);
            failedNodes += (failingNode);
        }

        on eProducersDone do {
            producersDone = true;
            if (producersDone && consumersDone) {
                goto Done;
            }
        }

        on eConsumersDone do {
            consumersDone = true;
            if (producersDone && consumersDone) {
                goto Done;
            }
        }
    }

    state Done {
        entry {    
            var i: int;
            send timer, halt;
            while (i < sizeof(logNodes)) {
                send logNodes[i], eShutDown, logNodes[i];
                i = i + 1;
            }
            raise halt;
        }

        // ignore eInformNodeFailure, eProducersDone, eConsumersDone, eInformNodeFailure, eTimeOut, eSegmentState, eEndSegmentResponse, eTakeOverLog, eNewSegment;
    }

    fun AcknowledgeNewSegment(client: Producer, logId: tLogId) {
        var log: Segment;
        log = CreateSegment(client);

        if (!(logId in logs)) {
            logs[logId] = default(seq[Segment]);
        }
        logs[logId] += (sizeof(logs[logId]), (log));

        UnreliableSend(client, eNewSegmentResponse, (status = NEW_SEG_OK, logId = logId, newSegment = sizeof(logs[logId])-1, nodes=log.logNodes, producer=client));
    }

    fun CreateSegment(producer: Producer): Segment {
        // Choose numReplicas nodes first
        var chosenNode: LogNode;
        var potentialNodes: set[LogNode];
        var chosenNodes: set[LogNode];

        potentialNodes = logNodes;
        while (sizeof(chosenNodes) < numReplicas) {
            chosenNode = choose(potentialNodes);
            potentialNodes -= (chosenNode);
            chosenNodes += (chosenNode);
        }
        return (logNodes = chosenNodes, producer = producer);
    }

    fun IsLastSegment(logId: tLogId, logSegment: int): tLastLogSegment {
        if (logSegment == -1) {
            if (logId in logs) {
                return (status = NOT_LAST, nodes = logs[logId][0].logNodes, producer = logs[logId][0].producer); 
            }

            return (status = IS_LAST, nodes = default(set[LogNode]), producer = default(Producer));
        }

        assert logId in logs;
        assert logSegment + 1 <= sizeof(logs[logId]);

        if (logSegment + 1 < sizeof(logs[logId])) {
            return (status = NOT_LAST, nodes = logs[logId][logSegment+1].logNodes, producer = logs[logId][logSegment+1].producer);
        }
        return (status = IS_LAST, nodes = logs[logId][sizeof(logs[logId]) - 1].logNodes, producer = default(Producer));
    }
}

fun BroadcastToNodes(nodes: set[LogNode], message: event, payload: any) {
    var i: int;
    while (i < sizeof(nodes)) {
        UnreliableSend(nodes[i], message, payload);
        i = i + 1;
    }
}
