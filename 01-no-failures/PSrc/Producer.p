/* 
The Producer will write all its log entries to the log nodes.
*/
machine Producer {
    var logNodes: set[LogNode];

    var currentEntry: int;
    var appendAcks: set[int];
    var numEntries: int;

    start state Init {
        entry (payload: (logNodes: set[LogNode], numEntries: int)) {
            logNodes = payload.logNodes;
            numEntries = payload.numEntries;
            currentEntry = 0;
            appendAcks = default(set[int]);
            goto Produce;
        }
    }

    state Produce {
        entry {
            var i: int;
            while (i < sizeof(logNodes)) {
                send logNodes[i], eAppendRequest, (client = this, seqNum = currentEntry, val = currentEntry);
                i = i + 1;
            }
        }

        on eAppendResponse do (response: tAppendResponse) {
            if (response.status == APPEND_OK) {
                appendAcks += (response.logNodeId);
                if (sizeof(appendAcks) < sizeof(logNodes)) {
                    return;
                }

                appendAcks = default(set[int]);
                currentEntry = currentEntry + 1;
                if (currentEntry == numEntries) {
                    goto Done;
                }
            }
            goto Produce;
        }
    }

    state Done {
        ignore eAppendResponse;
    }
}
