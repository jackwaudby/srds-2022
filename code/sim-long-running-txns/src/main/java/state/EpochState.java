package state;

public enum EpochState
{
    // processing job
    PROCESSING,

    // waiting for nodes to finish their current job
    WAITING,

    // executing two-phase commit
    COMMITTING,

    // executing an abort
    ABORTING,
}
