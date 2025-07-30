# AOS CS6210 Project 4 - MapReduce
## Shrey Wadhawan and Juan Macias Romero

# Assumptions and implementation notes
- All possible mapper-reducer pairs are considered when creating intermediate files. Each mapper will write to XX_YY.tx, where XX is the mapper_id and YY is the hash of the number of reducers. This means that, at most, a reducer will have to remote read M files. For adistributed file system, this could incur higher overheads, but on a local file system, this simplified the master's work significantly.

- For monitorization, we establsihed a parallel gRPC channel between master (client) and worker (server). They are pinged every 10 seconds and marked as FAILED if they don't respond. 

- Like in the original paper, we instantiated "Backup Tasks" to alleviate the problem of stragglers. Leveraging the pinging mechanism, if a worker doesn't respond for more than 5 seconds and it is marked as BUSY, a new worker is assigned its task (if AVAILABLE). This calls for data structure that keeps up with worker tasks and is resilient to duplicated/failed tasks. We used a map: task_to_worker_. A task is only considered done if the master is expecting it, and it discards any worker whose task has already been completed.

- There are R output files, even if the number of reducer workers is smaller or larger. Therefeore, each output file is called reduce_<task_id>.txt

- We implemented most of the worker logic in worker.h. mr_tasks.h was mainly for used for the emitting and flushing functions and for the partioning logic. We decided to batch all pairs in memory before flushing to disk, reducing I/O overheads.

## Other notes
- Output is based on keys, uppercase comes first.
- For sorting, we leveraged C++ map, which are ordered by default.


