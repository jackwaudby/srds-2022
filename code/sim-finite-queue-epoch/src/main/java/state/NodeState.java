package state;

public enum NodeState
{
    // up and processing jobs
    OPERATIONAL,

    // up but no job
    IDLE,

    // up and waiting to commit
    READY,

    // in failed commit group
    IN_FAILED_COMMIT_GROUP,

    // down
    CRASHED
}
