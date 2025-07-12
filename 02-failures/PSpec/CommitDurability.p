event eMonitor_CommitDurability: int;
type tLogEntryCommitted = (seqNum: tLogSeqNum, logId: int);
event eLogEntryCommitted: tLogEntryCommitted;

/*
Safety: Checks that only log replicas which were replicated to a sufficient 
number of nodes are considered committed.
*/
spec CommitDurability observes eLogEntryCommitted, eLogEntryStored, eMonitor_CommitDurability
{
	var logStoreEvents: map[tLogEntryCommitted, set[LogNode]];
	var requiredReplicas: int;

	start state Init {
		on eMonitor_CommitDurability goto WaitForEvents with (n: int) {
			requiredReplicas = n;
		}
	}

	state WaitForEvents {
		on eLogEntryStored do (resp: tLogEntryStored) {
			var logEntry: tLogEntryCommitted;
			logEntry = (seqNum = resp.seqNum, logId = resp.logId);

			if (!(logEntry in logStoreEvents)) {
				logStoreEvents[logEntry] = default(set[LogNode]);
			}
			logStoreEvents[logEntry] += (resp.logNode);
		}

		on eLogEntryCommitted do (resp: tLogEntryCommitted) {
			assert resp in logStoreEvents, 
			format("Seq number {0} was not stored to in log nodes before declared as committed", resp.seqNum);

			assert sizeof(logStoreEvents[resp]) >= requiredReplicas,
			format("Seq number {0} was not stored to in enough log nodes before declared as committed", requiredReplicas);
		}
	}
}


