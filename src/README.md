# MapReduce - Clement Habinshuti - CS 6210 Fall 2019

This MapReduce implementation is based on synchronous gRPC client and servers.
Each worker implements a synchronous gRPC server and the master manages a synchronous
gRPC client per worker. The master sends multiple jobs to different workers
simultaneously by making use of a threadpool. However each worker is single-threaded
and handles only one job at a time. All map jobs are completed first before
the the reduce tasks are executed.

## Main components

My implementation is based on the following major components:

### `WorkerClient`

The `WorkerClient` class encapsulates a gRPC client stub to a specific
worker server. It's used by the master to send map and reduce jobs
to a specific worker. It's also used to track the status of the worker,
which may be one of the following:
- `BUSY_MAP`: The worker is currently executing a map job
- `BUSY_REDUCE`: The worker is currently executing a reduce job
- `AVAILABLE`: The worker is idle and available to take up a job
- `PENDING`: The worker has been assigned a job but it has not yet started it. This
is a transitional state between `AVAILABLE` and `BUSY_*` and the worker cannot
be assigned another task in this state.
- `DEAD`: The master has determined that this worker is no longer usable or accessible
(e.g. due to connection loss, timeouts or errors)

### `WorkerPool`

The `WorkerPool` class manages and coordinates `WorkerClient`s. It's the main
workhorse of the master. It creates as many instances of `WorkerClient`s as there
are workers, and it also creates a threadpool with the same number of threads.

The `WorkerPool` creates a `MapJob` for each shard extracted from the input files
and places the job on a queue. It then processes a queue by popping the next job
from the queue, getting the next available worker and assigning it the job.
When all "alive" workers are busy, it waits for one of them to be available.
When a worker fails to complete a task successfully, that task is placed back
on the queue. If all workers die during the process, the `WorkerPool` terminates
and the master returns an error. Once all the tasks have been completed successfully
(i.e. queue is empty and no worker is busy), the map jobs have completed and the
reduce jobs can start. Reduce jobs are processed in a similar queue-based fashion.

The WorkerPool keeps track of the list of intermediate files returned from each
worker and creates `ReduceJob`s based on intermediate files with the same key.
Similary, workers return the output file of successful reduce jobs, and these files
are listed to the user upon completion of the entire MapReduce job.


### `threadpool`

Used by `WorkerPool` to send map/reduce jobs concurrently on multiple threads.
This threadpool implementation is adapted from the one submitted in Project 3.

### `master.h`

The master simply instantiates a `WorkerPool` based on the provided spec and uses
it to run the map and reduce jobs.

### `worker.h`

The `Worker` class implements a gRPC server that handles both map and reduce requests.
When executing a map request, the worker keeps track of all key-value
pairs emitted by the user' `map` function in an in-memory list. Once done, it
writes all the pairs in intermediate files. For a single map request, the worker
will create as many intermediate files as there are unique keys (same as `n_output_files` config). A given `<key:value>` pair will be written
to the file with the key `hash(key) mod n_output_files` (`std::hash` is used for hashing).

When executing a reduce job, the worker reads all the intermediate files
that it has been assigned, sort the key-value pairs by key, then pass
all the values related to each unique key to the user's `reduce` function.
The emitted values are kept in memory, then written to disk to an output file.

## Failure detection and error handling

The master library detects and handles two types of errors. The first type
are errors due to failure in processing the task, and the second type are gRPC
errors.

The system uses a boolean `success` field in response messages to determined
whether a task was successful. The worker sets this field to false when it
fails to read or write files or other similar errors. When the master gets
a response with the `success` flag set to false, it simply places the task
back on the queue and it will be retried later. If this is a recurring error
due to an underlying problem in the system (e.g. worker has no write permission
on output folder), it is the responsibility of the user to correct this issue,
otherwise the master will retry indefinitely.

gRPC errors are handled based on the specific error code from a RPC failed request:

- `UNAVAILABLE` (14): This occurs due to connection loss, or failure to connect to worker. When detected, the worker is marked as `DEAD` and won't be used again in future requests.
- `DEADLINE_EXCEEDED` (4): The master sets a deadline of 30s for the worker to respond.
    If this deadline is exceeded in 2 separate requests, with no successful request
    happening in between, then the worker is marked as `DEAD`.
- Other codes: If 10 consecutive requests to the worker fail, the worker is marked
as `DEAD`.

Regardless of whether or not the worker is marked as `DEAD`, the map/reduce job
will still be placed back to the queue to be retried later.

The master `run()` method terminates with `false` return value when all
workers are marked `DEAD`.

## Generated files

### Intermediate files

After each map job, a worker generates a number of intermediate files in the
specified output directory. Each file is named based on the pattern:
`<prefix>_<map_job_id>_<worker_id>_temp.txt`, where:
- `prefix`: All files with the same prefix will be sent to the same reducer.
The prefix is obtain from `hash(key) % n_output_files` where `key` comes
from an emitted key-value pair. `std::hash` is used as the hashing function
- `map_job_id`: an identifier assigned to each map job, from the range `0 <= job_id < num_shards`
- `worker_id`: an identifier for the worker based on the address and port the worker server binds to

This naming pattern ensures that no two workers access the same file, which
prevents collisions, race conditions. It also makes it safe for the master
to retry failed jobs even in the case where a worker continued to perform work
and write files even after the master considered the task as failed. Only the
map tasks that return successful responses to the master have their intermediate files
counted. And only these files are sent to reducers. Therefore there's no
problem with files with duplicate or incomplete content that were written
by failed workers.

Upon successful completion of the entire `MapReduce` job, all intermediate files
are deleted (including the ones not sent to the master) via the command:
```
rm <output_dir>/*temp*
```

### Final Output files

Output files from the reduce jobs are created in the output directory with the
name `<prefix>_result.txt` where `prefix` is in the range `0 <= prefix < n_output_files`. The contents of each result file are sorted by key in ascending order using
standard string comparison (uppercase letters appear before lowercase letters).

**Note**: do not add a trailing slash to the `output_dir` config option (spec validation will fail)