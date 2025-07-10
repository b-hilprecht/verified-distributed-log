event eMonitor_Durability: int;
event eMonitor_Progress: int;

/*
Safety: Checks that only log replicas which were replicated to a sufficient 
number of nodes are reported to consumers.
*/
spec Durability observes eLogEntryStored, eReadResponse, eMonitor_Durability
{
  var logStoreEvents: map[tLogSeqNum, int];
  var requiredReplicas: int;

  start state Init {
    on eMonitor_Durability goto WaitForEvents with (n: int) {
      requiredReplicas = n;
    }
  }

  state WaitForEvents {
    on eLogEntryStored do (resp: tLogEntryStored) {
      var seqNum : int;
      seqNum = resp.seqNum;

      if (!(seqNum in logStoreEvents)) {
        logStoreEvents[seqNum] = 1;
        return;
      }
      logStoreEvents[seqNum] = logStoreEvents[seqNum] + 1;
    }

    on eReadResponse do (resp: tReadResponse) {
      // No actual data is transmitted to the consumer. So this event is not relevant 
      // for durability.
      if (resp.status != READ_OK) {
        return;
      }

      assert resp.seqNum in logStoreEvents, 
      format("Seq number {0} was not stored before replied to a consumer", resp.seqNum);

      assert logStoreEvents[resp.seqNum] >= requiredReplicas,
      format("Sent log event to client that is not replicated to {0} replicas", requiredReplicas);
    }
  }
}

/* 
Liveness: checks that all log entries are eventually reported to a client.
*/
spec Progress observes eMonitor_Progress, eReadResponse {
  var reportedLogEntries: set[int];
  var numLogEntries: int;

  start state Init {
    on eMonitor_Progress goto WaitForReplies with (n: int) {
      numLogEntries = n;
    }
  }

  hot state WaitForReplies {
    on eReadResponse do (resp: tReadResponse) {
      if (resp.status != READ_OK) {
        return;
      }

      reportedLogEntries += (resp.seqNum);
      print format("Reported sequence number {0}", resp.seqNum);
      
      // We only expect all log entries up the last one to be reported
      // to the consumers.
      if (sizeof(reportedLogEntries) == numLogEntries - 1) {
        goto AllLogEntriesConsumed;
      }
    }
  }

  cold state AllLogEntriesConsumed {
    ignore eReadResponse;
  }
}
