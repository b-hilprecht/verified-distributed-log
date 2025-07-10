# P verification of distributed log

## Setup

Install the [P programming language](https://p-org.github.io/P/).

## Run

```bash
p compile 
p check

# run on a single seed
p compile && p check --fail-on-maxsteps --max-steps 100000 --seed 3

# run for a specific time
p compile && p check --fail-on-maxsteps --max-steps 100000 --seed 1 --timeout 800 --explore
```

## Run for all seeds and find a failing one

```bash
# on fish:
for seed in (seq 0 1000)
    echo "Running with seed: $seed"
    if not p check --fail-on-maxsteps --max-steps 100000 --seed $seed
        echo "Command failed with seed: $seed"
        break
    end
end
```
