event eMonitor_CommitDurability: int;
type tLogEntryCommitted = (producer: Producer, seqNum: tLogSeqNum, logId: int);
event eLogEntryCommitted: tLogEntryCommitted;

type tLogEntryCommit = (seqNum: tLogSeqNum, logId: tLogId);
event eDurableInCoordinator: tLogEntryCommit;

/*
Safety: Checks that only log replicas which were replicated to a sufficient 
number of nodes are considered committed.
*/
spec CommitDurability observes eLogEntryCommitted, eLogEntryStored, eDurableInCoordinator, eMonitor_CommitDurability
{
	var logStoreEvents: map[tLogEntryCommit, set[LogNode]];
	var durableInCoordinator: set[tLogEntryCommit];
	var requiredReplicas: int;

	start state Init {
		on eMonitor_CommitDurability goto WaitForEvents with (n: int) {
			requiredReplicas = n;
		}
	}

	state WaitForEvents {
		on eLogEntryStored do (resp: tLogEntryStored) {
			var logEntry: tLogEntryCommit;
			logEntry = (seqNum = resp.seqNum, logId = resp.logId);

			if (!(logEntry in logStoreEvents)) {
				logStoreEvents[logEntry] = default(set[LogNode]);
			}
			logStoreEvents[logEntry] += (resp.logNode);
		}

		on eLogEntryCommitted do (resp: tLogEntryCommitted) {
			var logEntry: tLogEntryCommit;
			logEntry = (seqNum = resp.seqNum, logId = resp.logId);

			if (logEntry in durableInCoordinator)
				return; 

			assert logEntry in logStoreEvents, 
			format("Seq number {0} was not stored to in log nodes before declared as committed", resp.seqNum);

			assert sizeof(logStoreEvents[logEntry]) >= requiredReplicas,
			format("Seq number {0} was not stored to in enough log nodes before declared as committed", requiredReplicas);
		}

		on eDurableInCoordinator do (resp: tLogEntryCommit) {
			durableInCoordinator += (resp);
		}
	}
}


