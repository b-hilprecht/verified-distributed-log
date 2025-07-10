/* 
Reads the log entries sequentially. Will retry if no new log entries were reported
yet.
*/
machine Consumer {
    var logNode: LogNode;
    var currentOffset: int;
    var totalNumLogEntries: int;
    var currNumLogEntries: int;

    start state Init {
        entry (payload: (logNode: LogNode, totalNumLogEntries: int)) {
            logNode = payload.logNode;
            totalNumLogEntries = payload.totalNumLogEntries;
            goto Consume;
        }
    }

    state Consume {
        entry {
            send logNode, eReadRequest, (client = this, offset = currentOffset);
        }

        on eReadResponse do (response: tReadResponse) {
            if (response.status == READ_OK) {
                currNumLogEntries = currNumLogEntries + 1;
                currentOffset = currentOffset + 1;
                if (currNumLogEntries == totalNumLogEntries) {
                    goto Done;
                }
            }
            goto Consume;
        }
    }

    state Done {
        ignore eReadResponse;
    }
}
