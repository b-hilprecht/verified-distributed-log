/*
The failure injector machine randomly selects a replica machine and enqueues the special event "halt".
*/
event eDelayNodeFailure;
// event: event sent by the failure injector to shutdown a node
event eShutDown: machine;
event eInformNodeFailure: LogNode;

machine FailureInjectorWithNotification {
  var nFailures: int;
  var nodes: set[LogNode];
  var coordinator: machine;

  start state Init {
    entry (config: (nodes: set[LogNode], nFailures: int, coordinator: machine)) {
      nFailures = config.nFailures;
      nodes = config.nodes;
      coordinator = config.coordinator;
      assert nFailures < sizeof(nodes);
      goto FailOneNode;
    }
  }

  state FailOneNode {
    entry {
      var fail: LogNode;

      if(nFailures == 0)
        raise halt; // done with all failures
      else
      {
        if(choose(10) == 0)
        {
          fail = choose(nodes);
          send fail, eShutDown, fail;
          send coordinator, eInformNodeFailure, fail;
          nodes -= (fail);
          nFailures = nFailures - 1;
        }
        else {
          send this, eDelayNodeFailure;
        }
      }
    }

    on eDelayNodeFailure goto FailOneNode;
  }
}

// function to create the failure injection
fun CreateFailureInjectorWithNotification(config: (nodes: set[LogNode], nFailures: int, coordinator: Coordinator)) {
  new FailureInjectorWithNotification(config);
}

// failure injector module
module FailureInjectorWithNotification = { FailureInjectorWithNotification };
