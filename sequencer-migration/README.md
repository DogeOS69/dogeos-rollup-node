# Scroll Sequencer Migration

This module contains documentation and scripts for the **one-way** Testnet migration of sequencing
from `l2geth` to the rollup node (RN) aka `l2reth`.

Under the Tsuki contract the transition is one-directional: geth has no Tsuki support and will not
receive it, the Testnet sequencer is the last geth node, and Mainnet never runs geth. After the
crossover, sequencing never returns to geth — there is no rollback or geth-recovery path.

### Risks
We want to minimize risks and service disruption. For this we need to consider the following risks:
- invalid L2 blocks produced
- L2 reorg (e.g. different blocks issued at the same L2 block height)
- L1 messages skipped/reverted
- general service interruption

## Migration Procedure (one-way)

The high-level flow of the crossover is:
1. `l2geth` is sequencing currently while a lagging `l2reth` follows it.
2. Freeze `l2geth` sequencing.
3. Record the frozen final `l2geth` block height/hash.
4. Wait until `l2reth` has reached that exact frozen final head.
5. Take an authoritative `l2geth` database snapshot/backup and shut `l2geth` sequencing down.
6. Enable `l2reth` sequencing.
7. Qualify the `l2reth` follower/sequencer with geth retired.

There is deliberately no step that re-enables `l2geth` sequencing or reverts `l2reth` to a prior
block: the last geth sequencer is the authoritative source only up to the frozen final head.

## Usage
Make sure the `L2RETH_RPC_URL` and `L2GETH_RPC_URL` env variables are properly configured, then run
the forward cutover script and follow the instructions.

```bash
./switch-to-l2reth.sh

# make common functions available in bash
source common-functions.sh

get_block_info $L2GETH_RPC_URL
[...]
```

### Testing locally
Run the one-way handoff integration test `docker_test_migrate_sequencer`. It starts with `l2geth`
sequencing, freezes it, proves all nodes reach geth's frozen final head, then hands sequencing to
`l2reth` for the remainder and asserts all nodes agree.

```bash
RUST_LOG=info,docker-compose=off cargo test --package tests --test migrate_sequencer -- docker_test_migrate_sequencer --exact --show-output

source local.env
./switch-to-l2reth.sh
```

### Running with Docker
```bash
docker run -it --rm sequencer-migration:latest

# then use the forward cutover script
./switch-to-l2reth.sh

# or call any of the common functions
get_block_info $L2GETH_RPC_URL
[...]
```

If running on Linux you might need to specify `-e L2GETH_RPC_URL=http://your-l2geth:8547 -e L2RETH_RPC_URL=http://your-l2reth:8545` as the default URLs might not work.
