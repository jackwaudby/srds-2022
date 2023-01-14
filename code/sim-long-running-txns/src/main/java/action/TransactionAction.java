package action;

import event.EventType;
import event.ReadyToCommitEvent;
import event.TransactionEvent;
import state.Cluster;
import state.NodeState;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class TransactionAction
{
    public static void execute( TransactionEvent event, Cluster cluster, Config config, EventList eventList, Rand rand )
    {
        // transaction has completed after its epoch has terminated -- ignore
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = event.getEpoch();
        var thisNodeId = event.getNodeId();

        if ( originEpoch < currentEpoch )
        {
            return;
        }

        var thisEventTime = event.getEventTime();

        switch ( cluster.getCurrentEpochState() )
        {
        // job completes whilst the epoch is processing
        case PROCESSING -> {
            switch ( cluster.getNodeState( thisNodeId ) )
            {
            // this node crashed before this job was completed
            case CRASHED -> {
                // counted when the failure event occurs
            }
            // happy path
            case OPERATIONAL -> {
                // if this job was distributed then
                if ( rand.isDistributedTransaction() )
                {
                    var affinity = config.isAffinity();
                    var dependencyNodeId = rand.generateDependency( thisNodeId, affinity );
                    var dependencyNodeState = cluster.getNodeState( dependencyNodeId );
                    switch ( dependencyNodeState )
                    {
                    // the dependency crashed during the job's execution then
                    case CRASHED -> cluster.incJobsLostAccessingCrashedNodes( thisNodeId );
                    // the dependency was operational during the job's execution then
                    case OPERATIONAL -> {
                        cluster.incCompletedJobs( thisNodeId, 1 );
                        // if multi-commit then need to add dependency
                        if ( Objects.equals( config.getAlgorithm(), "multi" ) )
                        {
                            cluster.addDependency( thisNodeId, dependencyNodeId );
                        }
                    }
                    }
                }
                // else this job was local then
                else
                {
                    cluster.incCompletedJobs( thisNodeId, 1 );
                }
            }
            }

            generateTransactionCompletionEvent( eventList, rand, currentEpoch, thisNodeId, thisEventTime );
        }
        // the epoch has timed out and now waiting for jobs to finish
        case WAITING -> {
            // all operational nodes (there can be a node that crashed prior to this epoch but has not repaired yet)
            // must have finished executing their current transaction before a commit operation event is generated
            if ( cluster.getNodeState( thisNodeId ) != NodeState.CRASHED )
            {
                cluster.setNodeState( thisNodeId, NodeState.READY );
                cluster.incCompletedJobs( thisNodeId, 1 );
                // if all operational nodes are commit then generate a commit completed operation event
                if ( cluster.areAllOperationalNodesReadyToCommit( config ) )
                {
                    eventList.addEvent( new ReadyToCommitEvent( thisEventTime, EventType.READY_TO_COMMIT, currentEpoch ) );
                }
            }
        }
        // there has been a failure, it was detected either when the epoch has timeout or during committing phase.
        case COMMITTING, ABORTING -> {
        }
        }
    }

    private static void generateTransactionCompletionEvent( EventList eventList, Rand rand, int currentEpoch, int thisNodeId, double thisEventTime )
    {
        var nextTransactionEventTime = thisEventTime + rand.generateTransactionServiceTime();
        var nextTransactionEvent = new TransactionEvent( nextTransactionEventTime, EventType.TRANSACTION_COMPLETED, thisNodeId, currentEpoch );
        eventList.addEvent( nextTransactionEvent );
    }
}
