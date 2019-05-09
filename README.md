# Sample job queue built with go and redis

It supports:

1. Queue
2. Client
3. Web Interface for schedulling
4. Client interface for management

It uses RPOPLPUSH redis command for building reliable queue

### How it works

1. Client adds a job in the workless:jobs:jobid list as a JobSpec json
2. Client sets a lock with the SETX workless:lock:jobid 1 60 duration dictated by the job spec
3. Client adds the jobid inside the workless:pending:quename
4. Worker using RPOPLPUSH moves the jobid into workless:processing:quename
5. Worker fetches the job from the workless:jobs:jobid
6. Worker when done removes the job from the workless:processing:quename using LREM workless:processing:quename 1 jobid
7. Worker removes the job using DEL workless:jobs:jobid
8. Worker Increments Stats
