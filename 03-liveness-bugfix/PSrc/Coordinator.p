// New segment
// previousSegmentId == -1 if this is the first log, logIds start with 0
type tNewSegment = (client: Producer, logId: tLogId, previousSegment: tSegmentId, previousSegmentNumEntries: int);
enum tNewSegmentResponseStatus { NEW_SEG_OK, NEW_SEG_ALREADY_APPENDED }
type tNewSegmentResponse = (status: tNewSegmentResponseStatus, logId: tLogId, newSegment: tSegmentId, nodes: set[LogNode]);
event eNewSegment : tNewSegment;
event eNewSegmentResponse : tNewSegmentResponse;

// End segment. Also contains how many log entries can be considered committed in the segment.
type tEndSegment = (coordinator: Coordinator, logId: tLogId, segment: tSegmentId, numEntries: int);
event eEndSegment : tEndSegment;
type tEndSegmentResponse = (node: LogNode, logId: tLogId, segment: tSegmentId, numEntries: int);
event eEndSegmentResponse : tEndSegmentResponse;

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
            lastLogSegment = IsLastSegment(req.logId, req.previousSegment);

            if (lastLogSegment.status == NOT_LAST) {
                // Could be that this is a retry to create the new segment
                if (lastLogSegment.producer == req.client) {
                    UnreliableSend(req.client, eNewSegmentResponse, (status = NEW_SEG_OK, logId = req.logId, newSegment = req.previousSegment + 1, nodes = lastLogSegment.nodes));        
                }
                UnreliableSend(req.client, eNewSegmentResponse, (status = NEW_SEG_ALREADY_APPENDED, logId = req.logId, newSegment = req.previousSegment + 1, nodes = default(set[LogNode])));
                return;
            }
            // For the first log segment, we do not have to end any previous p log segments
            // and can directly acknowledge the creation.
            if (req.previousSegment == -1) {
                AcknowledgeNewSegment(req.client, req.logId, req.previousSegment);
                return;
            }
            
            // The coordinator makes sure to only acknowledge a new segment once at least one 
            // log node confirms that the previous segment is closed. At this point, all writes
            // to the previous segment will fail (since the consumer expects all log nodes to 
            // reply with ok for a write to succeed).
            // Along with this request to end the previous log segment the producer also informs
            // how many entries in the segment can be considered committed. This then decides if
            // the last log entry will be returned to consumers or truncated.
            BroadcastToNodes(lastLogSegment.nodes, eEndSegment, (coordinator = this, logId = req.logId, segment = req.previousSegment, numEntries = req.previousSegmentNumEntries));
            
            // Keep track of which end segment requests need to be retried
            if (sizeof(unacknowledgedEndSegments) == 0)
                StartTimer(timer);
            while (i < sizeof(lastLogSegment.nodes)) {
                unacknowledgedEndSegments += ((node = lastLogSegment.nodes[i], logId = req.logId, segment = req.previousSegment, numEntries = req.previousSegmentNumEntries));
                i = i + 1;
            }
        }

        on eEndSegmentResponse do (req: tEndSegmentResponse) {
            assert req.logId in logs;

            unacknowledgedEndSegments -= (req);
            
            // it should concern the last log segment
            if (req.segment != sizeof(logs[req.logId])-1) {
                return;
            }

            AcknowledgeNewSegment(logs[req.logId][req.segment].producer, req.logId,  req.segment);
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

            // Retry unacknowledged end segments. Skip those directed to failed nodes
            while (i < sizeof(unacknowledgedEndSegments)) {
                segmentAck = unacknowledgedEndSegments[i];

                if (segmentAck.node in failedNodes) {
                    i = i + 1;
                    continue;
                }

                UnreliableSend(segmentAck.node, eEndSegment, (coordinator = this, logId = segmentAck.logId, segment = segmentAck.segment, numEntries = segmentAck.numEntries));
                newUnacknowledgedEndSegments += (segmentAck);
                i = i + 1;
            }
            if (sizeof(newUnacknowledgedEndSegments) > 0)
                StartTimer(timer);
            unacknowledgedEndSegments = newUnacknowledgedEndSegments;
        }

        on eShutDown do {
            var i: int;
            send timer, halt;
            while (i < sizeof(logNodes)) {
                send logNodes[i], eShutDown, logNodes[i];
                i = i + 1;
            }
            raise halt;
        }

        on eInformNodeFailure do (failingNode: LogNode) {
            logNodes -= (failingNode);
            failedNodes += (failingNode);
        }

        on eProducersDone do {
            producersDone = true;
            if (producersDone && consumersDone) {
                send this, eShutDown, this;
            }
        }

        on eConsumersDone do {
            consumersDone = true;
            if (producersDone && consumersDone) {
                send this, eShutDown, this;
            }
        }
    }

    fun AcknowledgeNewSegment(client: Producer, logId: tLogId, logSegment: tSegmentId) {
        var log: Segment;
        log = CreateSegment(client);

        if (!(logId in logs)) {
            logs[logId] = default(seq[Segment]);
        }
        logs[logId] += (sizeof(logs[logId]), (log));

        UnreliableSend(client, eNewSegmentResponse, (status = NEW_SEG_OK, logId = logId, newSegment = sizeof(logs[logId])-1, nodes=log.logNodes));
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
