package state;

public enum TerminalState
{
    // no nodes failed
    NO_FAILURE,

    // all nodes failed
    TOTAL_FAILURE,

    // at least 1 node failed
    PARTIAL_FAILURE,
}
