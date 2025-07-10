# Common techniques

This is almost a direct copy of the common components in the [P tutorial](https://github.com/p-org/P/tree/master/Tutorial/Common).

## Timers

However, as described in this [ticket](https://github.com/p-org/P/issues/876) sometimes we can get false positives in the timeout and this is fixed here.

Also, the probability of a failing send or kicking timer is reduced to 1/10 instead of 1/2. Otherwise the simulation takes too long.

## Failure injector

Moreover, the failure injector notifies a coordinator once a node fails. In a real system, this would be done by a control plane or failure detector. Note 
that this is not strictly necessary for the system to work. It's just an optimization to have fewer simulation steps described in more detail in the 
coordinator.
