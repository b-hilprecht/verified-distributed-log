test tcMultiReplica [main=MultiReplicaNoFailure]:
  assert Durability, Progress in
  (union Log, {MultiReplicaNoFailure});
