package action;

import event.EventType;
import event.ReadyToCommitEvent;
import event.TransactionEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class TransactionAction
{
    private final static Logger LOGGER = Logger.getLogger( TransactionAction.class.getName() );

    public static void execute( TransactionEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, ArrivalQueue queue )
    {
        // if transaction completes after its epoch has terminated then ignore
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = event.getEpoch();
        if ( originEpoch < currentEpoch )
        {
            return;
        }

        var thisNodeId = event.getNodeId();
        var currentEpochState = cluster.getCurrentEpochState();
        var thisEventTime = event.getEventTime();

        switch ( currentEpochState )
        {
        // job completes whilst the epoch is processing
        case PROCESSING -> {
            var thisNodeState = cluster.getNodeState( thisNodeId );
            switch ( thisNodeState )
            {
            // this node crashed before this job was completed handled when the failure event occurs
            case CRASHED -> {
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
                    case CRASHED -> {
                        cluster.incLostJobs( thisNodeId );
                        var currentJob = cluster.getCurrentJob( thisNodeId );
                        // ASSERT: to be generated the transaction must have had a job
                        if ( currentJob == null )
                        {
                            System.out.println();
                            System.out.println( "Should have had a job (TransactionAction 1)" );
                            System.exit( 1 );
                        }
                        currentJob.incRetries();
                        queue.addJob( currentJob );
                    }
                    // the dependency was operational during the job's execution then
                    case OPERATIONAL, IDLE -> {
                        cluster.incCompletedJobs( thisNodeId );
                        var completedJob = cluster.getCurrentJob( thisNodeId );
                        // ASSERT: to be generated the transaction must have had a job
                        if ( completedJob == null )
                        {
                            System.out.println();
                            System.out.println( "Should have had a job (TransactionAction 2)" );
                            System.exit( 1 );
                        }
                        cluster.addToCompletedJobStack( completedJob, thisNodeId );

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
                    cluster.incCompletedJobs( thisNodeId );
                    var completedJob = cluster.getCurrentJob( thisNodeId );
                    // ASSERT: to be generated the transaction must have had a job
                    if ( completedJob == null )
                    {
                        System.out.println();
                        System.out.println( "Should have had a job (TransactionAction 3)" );
                        System.exit( 1 );
                    }
                    cluster.addToCompletedJobStack( completedJob, thisNodeId );
                }

                // if the queue is not empty get a job
                Common.tryGetJobFromQueue( cluster, rand, eventList, queue, thisNodeId, thisEventTime, currentEpoch );
            }
            }
        }
        // case 3: the epoch has timed out and now waiting for jobs to finish
        case WAITING -> {
            // all operational nodes must have finished executing their current transaction before a commit operation event is generated

            // if the node was operational, i.e., did not crash during the epoch then
            if ( cluster.getNodeState( thisNodeId ) == NodeState.OPERATIONAL )
            {
                cluster.setNodeState( thisNodeId, NodeState.READY );
                cluster.incCompletedJobs( thisNodeId );
                var completedJob = cluster.getCurrentJob( thisNodeId );

                // ASSERT: to be generated the transaction must have had a job
                if ( completedJob == null )
                {
                    System.out.println();
                    System.out.println( "Should have had a job (TransactionAction 4)" );
                    System.exit( 1 );
                }

                cluster.addToCompletedJobStack( completedJob, thisNodeId );
                cluster.setCurrentJob( thisNodeId, null );

                // if all operational nodes are commit then generate a commit completed operation event
                if ( cluster.areAllOperationalNodesReadyToCommit( config ) )
                {
                    eventList.addEvent( new ReadyToCommitEvent( thisEventTime, EventType.READY_TO_COMMIT, currentEpoch ) );
                }
            }
        }
        // case 4: there has been a failure, it was detected either when the epoch has timeout or during committing phase.
        case COMMITTING, ABORTING -> {
        }
        }
    }
}
