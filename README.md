
# Toolbatch

utilities for orchestrating batches of jobs


## Components
- `Batch`: create a batch of jobs to be performed
- `Tracker`: track the completion status of each job
    - `FileTracker`: each job is tracked by a file that it saves
    - `SqlTracker`: each job is tracked in a sql database
    - *(planned)* `BucketTracker`: each job is tracked
    - *(planned)* `QueueTracker`: each job is tracked using a job queue
- `Executor`: execute jobs either directly or by delegating
    - `SerialExecutor`: execute jobs in serial on local machine
    - `ParallelExecutor`: execute jobs in parallel on local machine
    - *(planned)* `QueueExecutor`: submit jobs to a task queue
    - *(planned)* `FaasExecutor`: spawn FaaS jobs in cloud

